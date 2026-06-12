package session

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"BBgrid/BBgrid_Server/runtime"
	"BBgrid/common/proto"
	"BBgrid/common/store"

	"github.com/gorilla/websocket"
)

// TestRelaySimple 最简 relay 测试：source → server relay → target echo
func TestRelaySimple(t *testing.T) {
	storage, _ := store.NewStorageManager(store.StorageConfig{DataDir: t.TempDir()})
	core := runtime.NewCore(runtime.CoreConfig{PublicIP: "127.0.0.1"}, storage)
	srv := NewServer(core, "localhost", 0, nil)

	muxHTTP := http.NewServeMux()
	muxHTTP.HandleFunc("/relay", srv.HandleRelay)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	go http.Serve(ln, muxHTTP)
	addr := ln.Addr().String()

	sessionID := "s1"
	token := "t1"
	core.Publish(relayEvent(sessionID, "src", "tgt", "tcp", 8080, 22, "127.0.0.1", "127.0.0.1", token))
	time.Sleep(50 * time.Millisecond)

	// Target: 接收 → echo
	tgtWS := dialWS(t, addr, sessionID, token, "target", "tgt")
	defer tgtWS.Close()
	go func() {
		for {
			_, msg, err := tgtWS.ReadMessage()
			if err != nil {
				return
			}
			tgtWS.WriteMessage(websocket.BinaryMessage, msg)
		}
	}()

	// Source: 发送 → 接收
	srcWS := dialWS(t, addr, sessionID, token, "source", "src")
	defer srcWS.Close()

	data := []byte("hello relay test 1234567890")
	srcWS.WriteMessage(websocket.BinaryMessage, data)

	_, reply, err := srcWS.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}
	if !bytes.Equal(data, reply) {
		t.Errorf("expected %q, got %q", data, reply)
	} else {
		t.Logf("simple relay OK: %q", reply)
	}
}

// TestRelayWSMD5 100KB WS relay MD5 校验
func TestRelayWSMD5(t *testing.T) {
	storage, _ := store.NewStorageManager(store.StorageConfig{DataDir: t.TempDir()})
	core := runtime.NewCore(runtime.CoreConfig{PublicIP: "127.0.0.1"}, storage)
	srv := NewServer(core, "localhost", 0, nil)

	muxHTTP := http.NewServeMux()
	muxHTTP.HandleFunc("/relay", srv.HandleRelay)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	go http.Serve(ln, muxHTTP)
	addr := ln.Addr().String()

	sessionID := "s2"
	token := "t2"
	core.Publish(relayEvent(sessionID, "src", "tgt", "tcp", 8080, 22, "127.0.0.1", "127.0.0.1", token))
	time.Sleep(50 * time.Millisecond)

	tgtWS := dialWS(t, addr, sessionID, token, "target", "tgt")
	defer tgtWS.Close()
	go func() {
		buf := make([]byte, 65536)
		for {
			_, msg, err := tgtWS.ReadMessage()
			if err != nil {
				return
			}
			n := copy(buf, msg)
			tgtWS.WriteMessage(websocket.BinaryMessage, buf[:n])
		}
	}()

	srcWS := dialWS(t, addr, sessionID, token, "source", "src")
	defer srcWS.Close()

	// 100KB 数据
	dataSize := 100 * 1024
	sent := make([]byte, dataSize)
	rand.Read(sent)
	sentMD5 := md5.Sum(sent)

	// 发送 (32KB 分片)
	go func() {
		for i := 0; i < dataSize; i += 32 * 1024 {
			end := i + 32*1024
			if end > dataSize {
				end = dataSize
			}
			srcWS.WriteMessage(websocket.BinaryMessage, sent[i:end])
		}
	}()

	// 接收
	received := make([]byte, 0, dataSize)
	deadline := time.After(10 * time.Second)
	for len(received) < dataSize {
		select {
		case <-deadline:
			t.Fatalf("timeout: got %d/%d", len(received), dataSize)
		default:
		}
		_, msg, err := srcWS.ReadMessage()
		if err != nil {
			t.Fatalf("ReadMessage failed: %v", err)
		}
		received = append(received, msg...)
	}

	receivedMD5 := md5.Sum(received)
	if sentMD5 != receivedMD5 {
		t.Errorf("MD5 mismatch!")
	} else {
		t.Logf("WS MD5 OK: %s", hex.EncodeToString(sentMD5[:]))
	}
}

func dialWS(t *testing.T, addr, sessionID, token, role, clientID string) *websocket.Conn {
	t.Helper()
	url := fmt.Sprintf("ws://%s/relay?session=%s&token=%s&role=%s&client_id=%s",
		addr, sessionID, token, role, clientID)
	ws, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	return ws
}

func relayEvent(sessionID, source, target, protocol string, sourcePort, targetPort int, targetIP, sourceIP, token string) proto.GenericEvent {
	return proto.NewGenericEvent(
		proto.ResourceKey{Type: proto.ResourceTypeRelay, Namespace: proto.NamespaceDefault, Name: sessionID},
		proto.EventAdded,
		runtime.RelaySession{
			ID: sessionID, SourceClient: source, TargetClient: target, Protocol: protocol,
			SourcePort: sourcePort, TargetPort: targetPort,
			TargetLocalIP: targetIP, SourceLocalIP: sourceIP, Token: token,
		},
	)
}

// 保留这些空壳避免编译错误（其他测试引用了这些类型）
var _ = md5.Sum
var _ io.Reader
var _ sync.Mutex
