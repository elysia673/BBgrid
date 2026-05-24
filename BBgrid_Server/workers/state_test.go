package workers

import (
	"BBgrid/common/proto"
	"BBgrid/common/store"
	"os"
	"testing"
	"time"
)

type mockClientConn struct {
	messages []any
	closed   bool
}

func (m *mockClientConn) WriteJSON(v any) error {
	m.messages = append(m.messages, v)
	return nil
}

func (m *mockClientConn) Close() error {
	m.closed = true
	return nil
}

func (m *mockClientConn) GetHost() string        { return "test-host" }
func (m *mockClientConn) GetRemoteIP() string    { return "127.0.0.1" }
func (m *mockClientConn) IsTemp() bool           { return false }
func (m *mockClientConn) Latency() time.Duration { return 10 * time.Millisecond }
func (m *mockClientConn) LastPingAt() time.Time  { return time.Now() }

type mockDispatcher struct{}

func (d *mockDispatcher) SubscribeByType(resourceType string, handler func(proto.GenericEvent)) {}
func (d *mockDispatcher) Dispatch(event proto.GenericEvent)                                     {}

func newTestStateWorker(t *testing.T) *StateWorker {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "aether-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	dispatcher := &mockDispatcher{}
	storage, err := store.NewStorageManager(store.StorageConfig{
		DataDir:          tmpDir,
		SnapshotInterval: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { storage.Close() })

	return NewStateWorker(StateConfig{
		PublicIP:     "1.2.3.4",
		PingInterval: 30 * time.Second,
	}, dispatcher, storage)
}

func TestStateWorker_AddClient(t *testing.T) {
	state := newTestStateWorker(t)
	conn := &mockClientConn{}
	state.AddClient("client1", conn, "127.0.0.1:12345")

	client, ok := state.GetClient("client1")
	if !ok {
		t.Fatal("expected client to exist")
	}
	if client.ClientID() != "client1" {
		t.Errorf("expected clientID 'client1', got '%s'", client.ClientID())
	}

	clients := state.ListClients()
	if len(clients) != 1 {
		t.Errorf("expected 1 client, got %d", len(clients))
	}

	state.RemoveClient("client1")
	_, ok = state.GetClient("client1")
	if ok {
		t.Error("expected client to be removed")
	}
}

func TestStateWorker_Proxy(t *testing.T) {
	state := newTestStateWorker(t)
	conn := &mockClientConn{}
	state.AddClient("client1", conn, "127.0.0.1:12345")

	state.AddProxy("client1", ProxyState{
		RemotePort: 8080,
		LocalPort:  80,
		LocalIP:    "127.0.0.1",
		Protocol:   "tcp",
		BindAddr:   "0.0.0.0",
	})

	proxy, ok := state.GetProxy("client1", 8080)
	if !ok {
		t.Fatal("expected proxy to exist")
	}
	if proxy.LocalPort != 80 {
		t.Errorf("expected local port 80, got %d", proxy.LocalPort)
	}

	proxies := state.ListProxies()
	if len(proxies) != 1 {
		t.Errorf("expected 1 proxy, got %d", len(proxies))
	}

	state.RegisterPort("client1", 8080)
	clientID, ok := state.GetClientIDByPort(8080)
	if !ok || clientID != "client1" {
		t.Errorf("expected client1 for port 8080")
	}

	state.RemoveProxy("client1", 8080)
	_, ok = state.GetProxy("client1", 8080)
	if ok {
		t.Error("expected proxy to be removed")
	}
}

func TestStateWorker_TunnelToken(t *testing.T) {
	state := newTestStateWorker(t)
	conn := &mockClientConn{}
	state.AddClient("client1", conn, "127.0.0.1:12345")

	state.StoreTunnelToken("token1", "key1")
	_, key, err := state.FindTableByWSToken("token1")
	if err != nil {
		t.Fatalf("expected to find token, got error: %v", err)
	}
	if key != "key1" {
		t.Errorf("expected key 'key1', got '%s'", key)
	}

	state.RemoveTunnelTokenByKey("key1")
	_, _, err = state.FindTableByWSToken("token1")
	if err == nil {
		t.Error("expected error after removing token")
	}
}

func TestStateWorker_RelaySession(t *testing.T) {
	state := newTestStateWorker(t)

	session := RelaySession{
		ID:           "session1",
		SourceClient: "client1",
		TargetClient: "client2",
		Protocol:     "tcp",
		SourcePort:   8080,
		TargetPort:   8081,
		Token:        "token1",
		CreatedAt:    time.Now(),
		Status:       "connecting",
	}
	state.CreateRelaySession(session)

	s, ok := state.GetRelaySession("session1")
	if !ok {
		t.Fatal("expected session to exist")
	}
	if s.SourceClient != "client1" {
		t.Errorf("expected source client 'client1', got '%s'", s.SourceClient)
	}

	sessions := state.ListRelaySessions()
	if len(sessions) != 1 {
		t.Errorf("expected 1 session, got %d", len(sessions))
	}

	state.RemoveRelaySession("session1")
	_, ok = state.GetRelaySession("session1")
	if ok {
		t.Error("expected session to be removed")
	}
}

func TestStateWorker_PublicIP(t *testing.T) {
	state := newTestStateWorker(t)
	if state.GetPublicIP() != "1.2.3.4" {
		t.Errorf("expected public IP '1.2.3.4', got '%s'", state.GetPublicIP())
	}
}
