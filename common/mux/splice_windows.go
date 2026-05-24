//go:build windows

package mux

import (
	"fmt"
	"net"
)

// checkSpliceAvail Windows 不支持 splice/sendfile
func checkSpliceAvail(conn net.Conn) bool {
	return false
}

// spliceFd Windows 无内核态零拷贝，回退到用户空间
func spliceFd(inFd, outFd int, n int) (int, error) {
	return 0, fmt.Errorf("splice not supported on windows")
}
