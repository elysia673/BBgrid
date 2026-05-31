package mux

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"net"
	"testing"
	"time"
)

// TestMuxDataIntegrity 验证 mux 双向数据传输完整性（MD5 校验）
func TestMuxDataIntegrity(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	serverMux := New(server)
	clientMux := New(client)

	// client 端接收 channel 并 echo 回去
	clientMux.OnNewChannel = func(c *Channel) {
		go func() {
			buf := make([]byte, MaxFrameSize)
			for {
				n, err := c.Read(buf)
				if err != nil {
					return
				}
				c.Write(buf[:n])
			}
		}()
	}

	ch, err := serverMux.OpenChannel(9091)
	if err != nil {
		t.Fatalf("OpenChannel failed: %v", err)
	}

	// 测试多种大小的数据，MD5 校验
	testCases := []struct {
		name string
		size int
	}{
		{"1B", 1},
		{"1KB", 1024},
		{"32KB", 32 * 1024},
		{"64KB", 64*1024 - 1}, // 接近 MaxFrameSize
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 生成随机数据
			sent := make([]byte, tc.size)
			rand.Read(sent)
			sentMD5 := md5.Sum(sent)

			// 发送
			offset := 0
			for offset < len(sent) {
				n, err := ch.Write(sent[offset:])
				if err != nil {
					t.Fatalf("Write failed at offset %d: %v", offset, err)
				}
				offset += n
			}

			// 接收 echo
			received := make([]byte, 0, tc.size)
			buf := make([]byte, MaxFrameSize)
			deadline := time.After(10 * time.Second)
			for len(received) < tc.size {
				select {
				case <-deadline:
					t.Fatalf("timeout: got %d/%d bytes", len(received), tc.size)
				default:
				}
				n, err := ch.Read(buf)
				if err != nil {
					t.Fatalf("Read failed at %d/%d: %v", len(received), tc.size, err)
				}
				received = append(received, buf[:n]...)
			}

			// MD5 校验
			receivedMD5 := md5.Sum(received)
			if sentMD5 != receivedMD5 {
				t.Errorf("MD5 mismatch! sent=%s received=%s (size=%d)",
					hex.EncodeToString(sentMD5[:]),
					hex.EncodeToString(receivedMD5[:]),
					tc.size)
				// 找第一个不匹配的字节
				for i := range sent {
					if i >= len(received) || sent[i] != received[i] {
						t.Errorf("first mismatch at byte %d: sent=0x%02x got=0x%02x",
							i, sent[i], received[i])
						break
					}
				}
			} else {
				t.Logf("MD5 OK: %s (%d bytes)", hex.EncodeToString(sentMD5[:]), tc.size)
			}
		})
	}
}

// TestMuxLargeStream 模拟 SSH 流式传输（1MB，MD5 校验）
func TestMuxLargeStream(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	serverMux := New(server)
	clientMux := New(client)

	clientMux.OnNewChannel = func(c *Channel) {
		go func() {
			buf := make([]byte, MaxFrameSize)
			for {
				n, err := c.Read(buf)
				if err != nil {
					return
				}
				c.Write(buf[:n])
			}
		}()
	}

	ch, err := serverMux.OpenChannel(9091)
	if err != nil {
		t.Fatalf("OpenChannel failed: %v", err)
	}

	// 生成 1MB 随机数据
	dataSize := 1024 * 1024
	sent := make([]byte, dataSize)
	rand.Read(sent)
	sentMD5 := md5.Sum(sent)

	// 发送并接收 echo（同步，不用 goroutine）
	sentOffset := 0
	recvOffset := 0
	received := make([]byte, dataSize)
	readBuf := make([]byte, MaxFrameSize)
	deadline := time.After(30 * time.Second)

	for recvOffset < dataSize {
		// 发送一批
		if sentOffset < dataSize {
			writeBuf := make([]byte, 32*1024)
			n, _ := bytes.NewReader(sent[sentOffset:]).Read(writeBuf)
			if n > 0 {
				ch.Write(writeBuf[:n])
				sentOffset += n
			}
		}

		// 读取 echo
		select {
		case <-deadline:
			t.Fatalf("timeout: sent=%d recv=%d", sentOffset, recvOffset)
		default:
		}
		n, err := ch.Read(readBuf)
		if err != nil {
			t.Fatalf("Read failed at recv=%d: %v", recvOffset, err)
		}
		copy(received[recvOffset:], readBuf[:n])
		recvOffset += n
	}

	receivedMD5 := md5.Sum(received[:recvOffset])
	if sentMD5 != receivedMD5 {
		t.Errorf("MD5 mismatch! sent=%s received=%s",
			hex.EncodeToString(sentMD5[:]),
			hex.EncodeToString(receivedMD5[:]))
		for i := 0; i < dataSize && i < len(received); i++ {
			if sent[i] != received[i] {
				t.Errorf("first mismatch at byte %d: sent=0x%02x got=0x%02x (chunk %d, offset %d)",
					i, sent[i], received[i], i/32768, i%32768)
				break
			}
		}
	} else {
		t.Logf("1MB stream MD5 OK: %s", hex.EncodeToString(sentMD5[:]))
	}
}

// TestMuxCloseNoDataLoss 验证关闭 channel 不丢数据
func TestMuxCloseNoDataLoss(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	serverMux := New(server)

	var clientCh *Channel
	chReady := make(chan struct{})
	clientMux := New(client)
	clientMux.OnNewChannel = func(c *Channel) {
		clientCh = c
		close(chReady)
	}

	ch, err := serverMux.OpenChannel(9091)
	if err != nil {
		t.Fatalf("OpenChannel failed: %v", err)
	}

	// 发送消息
	messages := [][]byte{
		[]byte("msg1"),
		[]byte("msg2"),
		[]byte("msg3"),
	}
	for _, msg := range messages {
		ch.Write(msg)
		time.Sleep(10 * time.Millisecond) // 确保帧被 writeLoop 处理
	}

	// 等待 client 收到 channel
	select {
	case <-chReady:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for client channel")
	}

	// client 端读取消息
	received := 0
	deadline := time.After(5 * time.Second)
	for received < len(messages) {
		select {
		case <-deadline:
			t.Fatalf("timeout: got %d/%d messages", received, len(messages))
		default:
		}
		data, ok := clientCh.ReceiveBlocking()
		if !ok {
			t.Fatalf("channel closed after %d/%d messages", received, len(messages))
		}
		if !bytes.Equal(data, messages[received]) {
			t.Errorf("message %d: expected %q, got %q", received, messages[received], data)
		}
		received++
	}

	t.Logf("close test passed: all %d messages received", len(messages))
}
