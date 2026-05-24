package handler

import (
	alog "BBgrid/common/log"
	"BBgrid/common/mux"
	"io"
	"net"
	"sync"
	"time"
)

// bufferedConn 缓冲连接，优先从 reader 读取
type bufferedConn struct {
	reader io.Reader
	conn   net.Conn
}

func (bc *bufferedConn) Read(b []byte) (int, error)         { return bc.reader.Read(b) }
func (bc *bufferedConn) Write(b []byte) (int, error)        { return bc.conn.Write(b) }
func (bc *bufferedConn) Close() error                       { return bc.conn.Close() }
func (bc *bufferedConn) LocalAddr() net.Addr                { return bc.conn.LocalAddr() }
func (bc *bufferedConn) RemoteAddr() net.Addr               { return bc.conn.RemoteAddr() }
func (bc *bufferedConn) SetDeadline(t time.Time) error      { return bc.conn.SetDeadline(t) }
func (bc *bufferedConn) SetReadDeadline(t time.Time) error  { return bc.conn.SetReadDeadline(t) }
func (bc *bufferedConn) SetWriteDeadline(t time.Time) error { return bc.conn.SetWriteDeadline(t) }

// handleChannel 通过 mux 通道双向转发数据（WebSocket 隧道使用）
func handleChannel(publicConn net.Conn, channel *mux.Channel, key string) {
	defer func() {
		if r := recover(); r != nil {
			alog.Error(alog.CatMux, "handleChannel panic", "error", r, "port", channel.Port)
		}
		publicConn.Close()
	}()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				alog.Error(alog.CatMux, "handleChannel publicRead panic", "error", r, "port", channel.Port)
			}
			publicConn.Close()
			wg.Done()
		}()

		if tc, ok := publicConn.(*net.TCPConn); ok && channel.Mux.SpliceAvailable() {
			if f, err := tc.File(); err == nil {
				fd := int(f.Fd())
				defer f.Close()
				for {
					if err := channel.Mux.SpliceSend(channel.Port, fd, mux.MaxFrameSize); err != nil {
						break
					}
				}
				channel.Mux.CloseChannel(channel.Port)
				return
			}
		}

		buf := make([]byte, mux.MaxFrameSize)
		for {
			n, err := publicConn.Read(buf)
			if err != nil {
				if err != io.EOF {
					alog.Error(alog.CatMux, "public read error", "port", channel.Port, "error", err)
				}
				break
			}
			if err := channel.Mux.Send(channel.Port, buf[:n]); err != nil {
				alog.Error(alog.CatMux, "mux send error", "port", channel.Port, "error", err)
				break
			}
		}
		channel.Mux.CloseChannel(channel.Port)
	}()

	go func() {
		defer func() {
			if r := recover(); r != nil {
				alog.Error(alog.CatMux, "handleChannel publicWrite panic", "error", r, "port", channel.Port)
			}
			publicConn.Close()
			wg.Done()
		}()
		for {
			data, ok := channel.ReceiveBlocking()
			if !ok {
				break
			}
			if _, err := publicConn.Write(data); err != nil {
				alog.Error(alog.CatMux, "public write error", "port", channel.Port, "error", err)
				break
			}
			channel.Mux.SendWindowUpdate(channel.Port, len(data))
		}
	}()

	wg.Wait()
	alog.Info(alog.CatMux, "channel closed", "port", channel.Port, "key", key)
}
