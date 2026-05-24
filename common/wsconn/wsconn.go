// Package wsconn 将 WebSocket 连接适配为 net.Conn 接口
//
// 用于在 WebSocket 隧道上运行多路复用器。
// 内部处理 WebSocket 消息边界，对外提供流式读写。
package wsconn

import (
	"io"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// wsConn WebSocket 连接适配器
type wsConn struct {
	conn    *websocket.Conn
	reader  io.Reader // 当前消息读取器
	readMu  sync.Mutex
	writeMu sync.Mutex
	closeMu sync.Once
	closed  bool
}

// New 创建 WebSocket 连接适配器
func New(ws *websocket.Conn) net.Conn {
	return &wsConn{conn: ws}
}

// Read 读取数据
//
// 从 WebSocket 消息中读取数据，自动处理消息边界。
func (w *wsConn) Read(b []byte) (int, error) {
	w.readMu.Lock()
	defer w.readMu.Unlock()

	for {
		if w.reader != nil {
			n, err := w.reader.Read(b)
			if err == io.EOF {
				w.reader = nil
				continue
			}
			return n, err
		}

		_, msg, err := w.conn.ReadMessage()
		if err != nil {
			return 0, err
		}

		w.reader = &bytesReader{data: msg}
	}
}

func (w *wsConn) Write(b []byte) (int, error) {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	err := w.conn.WriteMessage(websocket.BinaryMessage, b)
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

func (w *wsConn) Close() error {
	w.closeMu.Do(func() {
		w.closed = true
	})
	return w.conn.Close()
}

func (w *wsConn) LocalAddr() net.Addr {
	return w.conn.LocalAddr()
}

func (w *wsConn) RemoteAddr() net.Addr {
	return w.conn.RemoteAddr()
}

func (w *wsConn) SetDeadline(t time.Time) error {
	return w.conn.SetReadDeadline(t)
}

func (w *wsConn) SetReadDeadline(t time.Time) error {
	return w.conn.SetReadDeadline(t)
}

func (w *wsConn) SetWriteDeadline(t time.Time) error {
	return w.conn.SetWriteDeadline(t)
}

type bytesReader struct {
	data []byte
	pos  int
}

func (r *bytesReader) Read(b []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(b, r.data[r.pos:])
	r.pos += n
	return n, nil
}
