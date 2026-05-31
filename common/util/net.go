package util

import (
	"io"
	"net"
	"sync"
)

func PipeBidir(a, b net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		io.Copy(a, b)
		if tc, ok := a.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
	}()

	go func() {
		defer wg.Done()
		io.Copy(b, a)
		if tc, ok := b.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
	}()

	wg.Wait()
	a.Close()
	b.Close()
}
