//go:build linux

package server

import "syscall"

type epoller struct {
	epfd         int
	wakeR, wakeW int
	events       []syscall.EpollEvent
	write        map[int]bool
}

func newPoller() (poller, error) {
	epfd, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return nil, err
	}
	r, w, err := newPipe()
	if err != nil {
		syscall.Close(epfd)
		return nil, err
	}
	p := &epoller{epfd: epfd, wakeR: r, wakeW: w, events: make([]syscall.EpollEvent, 256), write: map[int]bool{}}
	if err := p.ctl(syscall.EPOLL_CTL_ADD, r, syscall.EPOLLIN); err != nil {
		p.close()
		return nil, err
	}
	return p, nil
}

func (p *epoller) ctl(op, fd int, events uint32) error {
	ev := syscall.EpollEvent{Events: events, Fd: int32(fd)}
	return syscall.EpollCtl(p.epfd, op, fd, &ev)
}

func (p *epoller) add(fd int) error {
	return p.ctl(syscall.EPOLL_CTL_ADD, fd, syscall.EPOLLIN|syscall.EPOLLRDHUP)
}

func (p *epoller) setWrite(fd int, on bool) error {
	if p.write[fd] == on {
		return nil
	}
	p.write[fd] = on
	ev := uint32(syscall.EPOLLIN | syscall.EPOLLRDHUP)
	if on {
		ev |= syscall.EPOLLOUT
	}
	return p.ctl(syscall.EPOLL_CTL_MOD, fd, ev)
}

func (p *epoller) del(fd int) {
	delete(p.write, fd)
	syscall.EpollCtl(p.epfd, syscall.EPOLL_CTL_DEL, fd, nil)
}

func (p *epoller) wait(evs []pollEvent) (int, error) {
	max := len(evs)
	if max > len(p.events) {
		max = len(p.events)
	}
	n, err := syscall.EpollWait(p.epfd, p.events[:max], -1)
	if err != nil {
		return 0, err
	}
	for i := 0; i < n; i++ {
		e := p.events[i]
		evs[i] = pollEvent{
			fd:    int(e.Fd),
			read:  e.Events&(syscall.EPOLLIN|syscall.EPOLLRDHUP) != 0,
			write: e.Events&syscall.EPOLLOUT != 0,
			hup:   e.Events&(syscall.EPOLLHUP|syscall.EPOLLERR|syscall.EPOLLRDHUP) != 0,
		}
	}
	return n, nil
}

func (p *epoller) wake() error {
	_, err := syscall.Write(p.wakeW, []byte{1})
	if err == syscall.EAGAIN { // 管道已满说明已经有唤醒在排队
		return nil
	}
	return err
}

func (p *epoller) wakeFD() int { return p.wakeR }

func (p *epoller) drainWake() {
	var buf [256]byte
	for {
		if n, err := syscall.Read(p.wakeR, buf[:]); n <= 0 || err != nil {
			return
		}
	}
}

func (p *epoller) close() {
	syscall.Close(p.wakeR)
	syscall.Close(p.wakeW)
	syscall.Close(p.epfd)
}
