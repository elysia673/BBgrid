package main

import (
	"BBgrid/common/model"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"
)

func TestParseDataFromServer(t *testing.T) {
	wsMessage := `{"type":"proxy","data":"{\"server_host\":\"example.com\",\"remote_port\":17335,\"local_port\":25565}"}`

	var msg model.WSMessage
	if err := json.Unmarshal([]byte(wsMessage), &msg); err != nil {
		t.Fatal(err)
	}

	var cmd model.CommandData
	if dataStr, ok := msg.Data.(string); ok {
		if err := json.Unmarshal([]byte(dataStr), &cmd); err != nil {
			t.Fatal(err)
		}
	}

	t.Logf("After parsing: server_host=%s, remote_port=%d", cmd.ServerHost, cmd.RemotePort)

	if cmd.ServerHost == "" {
		t.Error("server_host is empty!")
	}
	if cmd.RemotePort != 17335 {
		t.Errorf("Expected remote_port=17335, got %d", cmd.RemotePort)
	}
}

func TestFullProxyFlow(t *testing.T) {
	lnTunnel, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lnTunnel.Close()
	tunnelPort := lnTunnel.Addr().(*net.TCPAddr).Port

	lnLocal, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lnLocal.Close()
	localPort := lnLocal.Addr().(*net.TCPAddr).Port

	tunnelServerCh := make(chan net.Conn, 1)
	localServerCh := make(chan net.Conn, 1)

	go func() {
		conn, err := lnTunnel.Accept()
		if err != nil {
			return
		}
		tunnelServerCh <- conn
	}()

	go func() {
		conn, err := lnLocal.Accept()
		if err != nil {
			return
		}
		localServerCh <- conn
	}()

	time.Sleep(50 * time.Millisecond)

	cmd := model.CommandData{
		ServerHost: "127.0.0.1",
		RemotePort: tunnelPort,
		LocalPort:  localPort,
	}
	cmdBytes, _ := json.Marshal(cmd)
	proxyCmd := model.WSMessage{
		Type: "proxy",
		Data: string(cmdBytes),
	}
	dataBytes, _ := json.Marshal(proxyCmd)

	var parsed model.WSMessage
	json.Unmarshal(dataBytes, &parsed)

	var cmd2 model.CommandData
	if dataStr, ok := parsed.Data.(string); ok {
		json.Unmarshal([]byte(dataStr), &cmd2)
	}

	tunnelClientConn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", cmd2.RemotePort))
	if err != nil {
		t.Fatal(err)
	}
	defer tunnelClientConn.Close()

	localClientConn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", cmd2.LocalPort))
	if err != nil {
		t.Fatal(err)
	}
	defer localClientConn.Close()

	tunnelServerConn := <-tunnelServerCh
	if tunnelServerConn == nil {
		t.Fatal("tunnel not established")
	}
	defer tunnelServerConn.Close()

	localServerConn := <-localServerCh
	if localServerConn == nil {
		t.Fatal("local service connection not established")
	}
	defer localServerConn.Close()

	t.Log("All connections established")

	tunnelClientConn.Write([]byte("hello"))
	buf := make([]byte, 1024)
	tunnelServerConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := tunnelServerConn.Read(buf)
	if err != nil {
		t.Fatalf("tunnel read failed: %v", err)
	}
	if string(buf[:n]) != "hello" {
		t.Errorf("tunnel data mismatch: got '%s'", string(buf[:n]))
	}

	localClientConn.Write([]byte("world"))
	localServerConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err = localServerConn.Read(buf)
	if err != nil {
		t.Fatalf("local read failed: %v", err)
	}
	if string(buf[:n]) != "world" {
		t.Errorf("local data mismatch: got '%s'", string(buf[:n]))
	}

	t.Log("PASSED: Full proxy flow works!")
}
