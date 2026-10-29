//go:build linux || darwin || freebsd || netbsd || openbsd

package server

import (
	"fmt"
	"net"
	"strconv"
	"syscall"
)

const eventLoopSupported = true

type pollEvent struct {
	fd    int
	read  bool
	write bool
	hup   bool
}

// poller 是 epoll / kqueue 的统一抽象 (电平触发)。
type poller interface {
	add(fd int) error               // 注册读事件
	setWrite(fd int, on bool) error // 开关写事件
	del(fd int)
	wait(evs []pollEvent) (int, error)
	wake() error // 从其他 goroutine 唤醒阻塞中的 wait
	wakeFD() int
	drainWake()
	close()
}

// newPipe 创建非阻塞管道，用于跨 goroutine 唤醒事件循环。
func newPipe() (r, w int, err error) {
	var fds [2]int
	if err = syscall.Pipe(fds[:]); err != nil {
		return
	}
	for _, fd := range fds {
		syscall.CloseOnExec(fd)
		if err = syscall.SetNonblock(fd, true); err != nil {
			syscall.Close(fds[0])
			syscall.Close(fds[1])
			return
		}
	}
	return fds[0], fds[1], nil
}

// listenSocket 创建非阻塞的 IPv4 监听套接字，返回 fd 和实际地址。
func listenSocket(addr string) (int, string, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return 0, "", err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 0, "", err
	}
	ip := net.ParseIP(host)
	if host == "" {
		ip = net.IPv4zero
	}
	if ip == nil {
		ips, lerr := net.LookupIP(host)
		if lerr != nil || len(ips) == 0 {
			return 0, "", fmt.Errorf("cannot resolve %q", host)
		}
		ip = ips[0]
	}
	ip4 := ip.To4()
	if ip4 == nil {
		return 0, "", fmt.Errorf("event loop supports IPv4 only: %q", host)
	}
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		return 0, "", err
	}
	syscall.CloseOnExec(fd)
	fail := func(err error) (int, string, error) { syscall.Close(fd); return 0, "", err }
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		return fail(err)
	}
	sa := &syscall.SockaddrInet4{Port: port}
	copy(sa.Addr[:], ip4)
	if err := syscall.Bind(fd, sa); err != nil {
		return fail(err)
	}
	if err := syscall.Listen(fd, 1024); err != nil {
		return fail(err)
	}
	if err := syscall.SetNonblock(fd, true); err != nil {
		return fail(err)
	}
	local, err := syscall.Getsockname(fd)
	if err != nil {
		return fail(err)
	}
	in4 := local.(*syscall.SockaddrInet4)
	return fd, net.JoinHostPort(net.IP(in4.Addr[:]).String(), strconv.Itoa(in4.Port)), nil
}

// acceptConn 接受一个连接并设为非阻塞 + TCP_NODELAY。
func acceptConn(lfd int) (int, error) {
	nfd, _, err := syscall.Accept(lfd)
	if err != nil {
		return -1, err
	}
	syscall.CloseOnExec(nfd)
	if err := syscall.SetNonblock(nfd, true); err != nil {
		syscall.Close(nfd)
		return -1, err
	}
	syscall.SetsockoptInt(nfd, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
	return nfd, nil
}
