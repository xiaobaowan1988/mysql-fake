//go:build !linux && !darwin && !freebsd && !netbsd && !openbsd

package server

const eventLoopSupported = false

type eventLoop struct{ addr string }

func newEventLoop(*Server) (*eventLoop, error) { return nil, ErrEventLoopUnsupported }
func (l *eventLoop) run()                      {}
func (l *eventLoop) stop()                     {}
