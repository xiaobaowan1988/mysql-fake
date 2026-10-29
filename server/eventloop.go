//go:build linux || darwin || freebsd || netbsd || openbsd

package server

import (
	"sync"
	"sync/atomic"
	"syscall"
)

// evConn 是事件循环里的一个连接。除 pc 之外的所有字段只会被事件循环 goroutine 访问；
// pc 在任何时刻最多被一个工作池任务使用 (running 标志保证)。
type evConn struct {
	fd              int
	pc              *protoConn
	inbuf           []byte
	out             []byte
	pending         []packet
	running         bool // 有请求正在工作池里执行
	dead            bool // 对端关闭或出错: 等执行中的请求结束后关闭
	closeAfterFlush bool
	writing         bool
	closed          bool
}

// eventLoop 是单线程的 Reactor: 只做 accept、非阻塞读写和协议拆包；
// 完整的请求投递给动态工作池执行，结果通过 post 回到循环线程再写出。
type eventLoop struct {
	srv   *Server
	p     poller
	lfd   int
	addr  string
	conns map[int]*evConn

	mu     sync.Mutex
	posted []func()
	quit   atomic.Bool
	done   chan struct{}
}

func newEventLoop(s *Server) (*eventLoop, error) {
	lfd, addr, err := listenSocket(s.cfg.Addr)
	if err != nil {
		return nil, err
	}
	p, err := newPoller()
	if err != nil {
		syscall.Close(lfd)
		return nil, err
	}
	if err := p.add(lfd); err != nil {
		p.close()
		syscall.Close(lfd)
		return nil, err
	}
	return &eventLoop{srv: s, p: p, lfd: lfd, addr: addr, conns: map[int]*evConn{}, done: make(chan struct{})}, nil
}

// post 让 f 在事件循环线程里执行 (线程安全)。
func (l *eventLoop) post(f func()) {
	l.mu.Lock()
	l.posted = append(l.posted, f)
	l.mu.Unlock()
	l.p.wake()
}

func (l *eventLoop) stop() {
	l.quit.Store(true)
	l.p.wake()
	<-l.done
}

func (l *eventLoop) run() {
	defer close(l.done)
	events := make([]pollEvent, 256)
	for !l.quit.Load() {
		n, err := l.p.wait(events)
		if err != nil {
			if err == syscall.EINTR {
				continue
			}
			break
		}
		for i := 0; i < n; i++ {
			ev := events[i]
			switch {
			case ev.fd == l.p.wakeFD():
				l.p.drainWake()
			case ev.fd == l.lfd:
				l.accept()
			default:
				c := l.conns[ev.fd]
				if c == nil || c.closed {
					continue
				}
				if ev.read || ev.hup {
					l.onReadable(c)
				}
				if ev.write && !c.closed {
					l.flush(c)
				}
			}
		}
		l.runPosted()
	}
	// 退出: 关闭所有连接和监听套接字
	l.runPosted()
	for _, c := range l.conns {
		l.p.del(c.fd)
		syscall.Close(c.fd)
		c.closed = true
		if !c.running { // 仍在执行的请求结束后由 Server.Close 统一回收会话
			l.srv.releaseConn(c.pc)
		}
	}
	l.conns = nil
	syscall.Close(l.lfd)
	l.p.close()
}

func (l *eventLoop) runPosted() {
	l.mu.Lock()
	fs := l.posted
	l.posted = nil
	l.mu.Unlock()
	for _, f := range fs {
		f()
	}
}

func (l *eventLoop) accept() {
	for i := 0; i < 128; i++ {
		fd, err := acceptConn(l.lfd)
		if err != nil {
			return // EAGAIN 或其他错误: 等待下一次事件
		}
		s := l.srv
		if s.cfg.MaxConns > 0 && int(s.nconns.Load()) >= s.cfg.MaxConns {
			var seq byte
			syscall.Write(fd, framePacket(nil, &seq, errPayload(errTooManyConns, "08004", "Too many connections")))
			syscall.Close(fd)
			s.rejected.Add(1)
			continue
		}
		pc := newProtoConn(s.cfg.Engine)
		c := &evConn{fd: fd, pc: pc}
		if err := l.p.add(fd); err != nil {
			syscall.Close(fd)
			continue
		}
		l.conns[fd] = c
		s.track(pc)
		c.out = pc.greeting()
		l.flush(c)
	}
}

func (l *eventLoop) onReadable(c *evConn) {
	var buf [32 << 10]byte
	for i := 0; i < 16; i++ { // 限制单次处理量，避免一个连接独占循环 (电平触发会再次通知)
		n, err := syscall.Read(c.fd, buf[:])
		if err == syscall.EAGAIN {
			break
		}
		if err == syscall.EINTR {
			continue
		}
		if err != nil || n == 0 {
			c.dead = true
			break
		}
		c.inbuf = append(c.inbuf, buf[:n]...)
	}
	if !c.dead {
		pkts, rest, err := parsePackets(c.inbuf)
		if err != nil {
			c.dead = true
		} else {
			c.inbuf = append(c.inbuf[:0], rest...)
			c.pending = append(c.pending, pkts...)
		}
	}
	if c.dead {
		c.pending = nil
	}
	l.dispatch(c)
	l.maybeClose(c)
}

// dispatch 把连接的下一个待处理请求交给工作池 (同一连接串行)。
func (l *eventLoop) dispatch(c *evConn) {
	if c.running || c.dead || c.closed || len(c.pending) == 0 {
		return
	}
	pkt := c.pending[0]
	c.pending = c.pending[1:]
	c.running = true
	ok := l.srv.pool.Submit(func() {
		resp, closeAfter := c.pc.handle(pkt)
		l.post(func() {
			c.running = false
			if c.closed {
				return
			}
			c.out = append(c.out, resp...)
			if closeAfter {
				c.closeAfterFlush = true
				c.pending = nil
			}
			l.flush(c)
			l.dispatch(c)
			l.maybeClose(c)
		})
	})
	if !ok {
		c.running, c.dead = false, true
	}
}

// flush 尽量把输出缓冲写出；写不完则开启写事件，写完则关闭。
func (l *eventLoop) flush(c *evConn) {
	for len(c.out) > 0 {
		n, err := syscall.Write(c.fd, c.out)
		if err == syscall.EAGAIN {
			if !c.writing {
				c.writing = true
				l.p.setWrite(c.fd, true)
			}
			return
		}
		if err == syscall.EINTR {
			continue
		}
		if err != nil {
			c.dead = true
			c.out = nil
			break
		}
		c.out = c.out[n:]
	}
	if len(c.out) == 0 {
		c.out = nil
		if c.writing {
			c.writing = false
			l.p.setWrite(c.fd, false)
		}
	}
	l.maybeClose(c)
}

func (l *eventLoop) maybeClose(c *evConn) {
	if c.closed || c.running {
		return
	}
	if c.dead || (c.closeAfterFlush && len(c.out) == 0) {
		l.closeConn(c)
	}
}

func (l *eventLoop) closeConn(c *evConn) {
	c.closed = true
	l.p.del(c.fd)
	syscall.Close(c.fd)
	delete(l.conns, c.fd)
	pc := c.pc
	// 会话回滚可能需要 I/O，交给工作池，不阻塞事件循环
	if !l.srv.pool.Submit(func() { l.srv.releaseConn(pc) }) {
		l.srv.releaseConn(pc)
	}
}
