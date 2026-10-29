//go:build darwin || freebsd || netbsd || openbsd

package server

import "syscall"

type kqueuer struct {
	kq           int
	wakeR, wakeW int
	events       []syscall.Kevent_t
	write        map[int]bool
}

func newPoller() (poller, error) {
	kq, err := syscall.Kqueue()
	if err != nil {
		return nil, err
	}
	syscall.CloseOnExec(kq)
	r, w, err := newPipe()
	if err != nil {
		syscall.Close(kq)
		return nil, err
	}
	p := &kqueuer{kq: kq, wakeR: r, wakeW: w, events: make([]syscall.Kevent_t, 256), write: map[int]bool{}}
	if err := p.change(r, syscall.EVFILT_READ, syscall.EV_ADD); err != nil {
		p.close()
		return nil, err
	}
	return p, nil
}

func (p *kqueuer) change(fd int, filter int16, flags int) error {
	var k syscall.Kevent_t
	syscall.SetKevent(&k, fd, int(filter), flags)
	_, err := syscall.Kevent(p.kq, []syscall.Kevent_t{k}, nil, nil)
	return err
}

func (p *kqueuer) add(fd int) error { return p.change(fd, syscall.EVFILT_READ, syscall.EV_ADD) }

func (p *kqueuer) setWrite(fd int, on bool) error {
	if p.write[fd] == on {
		return nil
	}
	p.write[fd] = on
	if on {
		return p.change(fd, syscall.EVFILT_WRITE, syscall.EV_ADD)
	}
	err := p.change(fd, syscall.EVFILT_WRITE, syscall.EV_DELETE)
	if err == syscall.ENOENT {
		return nil
	}
	return err
}

func (p *kqueuer) del(fd int) {
	delete(p.write, fd)
	p.change(fd, syscall.EVFILT_READ, syscall.EV_DELETE)
	p.change(fd, syscall.EVFILT_WRITE, syscall.EV_DELETE)
}

func (p *kqueuer) wait(evs []pollEvent) (int, error) {
	max := len(evs)
	if max > len(p.events) {
		max = len(p.events)
	}
	n, err := syscall.Kevent(p.kq, nil, p.events[:max], nil)
	if err != nil {
		return 0, err
	}
	for i := 0; i < n; i++ {
		e := p.events[i]
		evs[i] = pollEvent{
			fd:    int(e.Ident),
			read:  e.Filter == syscall.EVFILT_READ,
			write: e.Filter == syscall.EVFILT_WRITE,
			hup:   e.Flags&(syscall.EV_EOF|syscall.EV_ERROR) != 0,
		}
	}
	return n, nil
}

func (p *kqueuer) wake() error {
	_, err := syscall.Write(p.wakeW, []byte{1})
	if err == syscall.EAGAIN {
		return nil
	}
	return err
}

func (p *kqueuer) wakeFD() int { return p.wakeR }

func (p *kqueuer) drainWake() {
	var buf [256]byte
	for {
		if n, err := syscall.Read(p.wakeR, buf[:]); n <= 0 || err != nil {
			return
		}
	}
}

func (p *kqueuer) close() {
	syscall.Close(p.wakeR)
	syscall.Close(p.wakeW)
	syscall.Close(p.kq)
}
