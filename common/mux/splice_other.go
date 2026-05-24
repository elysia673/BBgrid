//go:build !linux && !darwin && !windows

package mux

import (
	"fmt"
	"net"
)

// checkSpliceAvail 其他平台不支持 splice/sendfile
func checkSpliceAvail(conn net.Conn) bool {
	return false
}

// spliceFd 其他平台无内核态零拷贝
func spliceFd(inFd, outFd int, n int) (int, error) {
	return 0, fmt.Errorf("splice not supported on this platform")
}
