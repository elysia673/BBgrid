//go:build linux

package mux

import (
	"net"
	"syscall"
	"time"
)

const (
	spliceFMove     = 0x01
	spliceFNonblock = 0x02
)

// checkSpliceAvail 检测 splice 是否可用
func checkSpliceAvail(conn net.Conn) bool {
	tc, ok := conn.(*net.TCPConn)
	if !ok {
		return false
	}
	f, err := tc.File()
	if err != nil {
		return false
	}
	fd := int(f.Fd())
	f.Close()

	var fds [2]int
	if err := syscall.Pipe(fds[:]); err != nil {
		return false
	}
	defer syscall.Close(fds[0])
	defer syscall.Close(fds[1])

	_, _, errno := syscall.Syscall6(
		syscall.SYS_SPLICE,
		uintptr(fd), 0,
		uintptr(fds[1]), 0,
		1,
		uintptr(spliceFMove|spliceFNonblock),
	)
	return errno == 0 || errno == syscall.EAGAIN
}

// spliceFd splice 系统调用（非阻塞，EAGAIN 重试）
func spliceFd(inFd, outFd int, n int) (int, error) {
	for {
		n64, _, errno := syscall.Syscall6(
			syscall.SYS_SPLICE,
			uintptr(inFd), 0,
			uintptr(outFd), 0,
			uintptr(n),
			uintptr(spliceFMove|spliceFNonblock),
		)
		if errno == 0 {
			return int(n64), nil
		}
		if errno == syscall.EAGAIN {
			time.Sleep(time.Millisecond)
			continue
		}
		return 0, errno
	}
}
