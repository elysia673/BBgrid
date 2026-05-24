//go:build darwin

package mux

import (
	"net"
	"syscall"
)

// checkSpliceAvail macOS 不支持 splice
func checkSpliceAvail(conn net.Conn) bool {
	return false
}

// spliceFd macOS 用 sendfile 替代（socket→socket 零拷贝）
// sendfile 在 macOS 上支持 socket→socket 直接传输
func spliceFd(inFd, outFd int, n int) (int, error) {
	return syscall.Sendfile(outFd, inFd, nil, n)
}
