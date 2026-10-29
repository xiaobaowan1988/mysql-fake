package server

import (
	"errors"
	"io"
	"net"
	"sync"

	"mysqlfake/metrics"
)

// serveGoroutines: 一连接一个 goroutine 的传统模型。协议处理仍交给动态工作池执行，
// 因此"并发执行的语句数"同样受 MaxWorkers 限制。
func (s *Server) serveGoroutines() {
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		nc, err := s.ln.Accept()
		if err != nil {
			return
		}
		if s.cfg.MaxConns > 0 && int(s.nconns.Load()) >= s.cfg.MaxConns {
			var seq byte
			nc.Write(framePacket(nil, &seq, errPayload(errTooManyConns, "08004", "Too many connections")))
			nc.Close()
			s.rejected.Add(1)
			metrics.ConnRejected.Inc()
			continue
		}
		if tc, ok := nc.(*net.TCPConn); ok {
			tc.SetNoDelay(true)
		}
		pc := newProtoConn(s.cfg.Engine)
		pc.netConn = nc
		s.track(pc)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer nc.Close()
			defer s.untrack(pc)
			s.serveConn(nc, pc)
		}()
	}
}

func (s *Server) serveConn(nc net.Conn, pc *protoConn) {
	if _, err := nc.Write(pc.greeting()); err != nil {
		return
	}
	var buf []byte
	tmp := make([]byte, 32<<10)
	for {
		n, err := nc.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
			pkts, rest, perr := parsePackets(buf)
			if perr != nil {
				return
			}
			buf = append(buf[:0], rest...)
			for _, p := range pkts {
				type result struct {
					resp  []byte
					close bool
				}
				ch := make(chan result, 1)
				if !s.pool.Submit(func() {
					r, c := pc.handle(p)
					ch <- result{r, c}
				}) {
					return
				}
				res := <-ch
				if len(res.resp) > 0 {
					if _, werr := nc.Write(res.resp); werr != nil {
						return
					}
				}
				if res.close {
					return
				}
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return
			}
			return
		}
	}
}
