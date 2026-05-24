package main

import (
	"fmt"
	"os/exec"
	"strings"
	"sync"

	"BBgrid/common/sdk"
)

// VPNManager 管理 WireGuard 接口的生命周期
type VPNManager struct {
	mu     sync.Mutex
	ns     map[string]*namespaceState
	ifaces map[string]*ifaceState
	dry    bool
}

type namespaceState struct {
	Name    string
	Members map[string]map[string]any
}

type ifaceState struct {
	Name      string
	Namespace string
	IP        string
	Peers     map[string]*peerState
}

type peerState struct {
	ClientID   string
	PublicKey  string
	Endpoint   string
	AllowedIPs []string
}

func NewVPNManager(dryRun bool) *VPNManager {
	return &VPNManager{
		ns:     make(map[string]*namespaceState),
		ifaces: make(map[string]*ifaceState),
		dry:    dryRun,
	}
}

func (m *VPNManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for name := range m.ifaces {
		m.deleteInterface(name)
	}
}

func (m *VPNManager) GetStatus() map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	namespaces := make([]string, 0, len(m.ns))
	for name := range m.ns {
		namespaces = append(namespaces, name)
	}
	ifaces := make([]string, 0, len(m.ifaces))
	for name := range m.ifaces {
		ifaces = append(ifaces, name)
	}
	return map[string]any{"namespaces": namespaces, "interfaces": ifaces, "dry_run": m.dry}
}

func (m *VPNManager) HandleNamespaceAdded(event sdk.Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	name := event.Resource.Name
	if _, exists := m.ns[name]; exists {
		return
	}
	m.ns[name] = &namespaceState{Name: name, Members: make(map[string]map[string]any)}
	m.createInterface("wg-"+name, name)
	vpnLog("namespace-add", name, "")
}

func (m *VPNManager) HandleNamespaceModified(event sdk.Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	name := event.Resource.Name
	ns, exists := m.ns[name]
	if !exists {
		return
	}
	if members, ok := event.Payload["members"].([]any); ok {
		for _, member := range members {
			if memberMap, ok := member.(map[string]any); ok {
				if clientID, ok := memberMap["client_id"].(string); ok {
					ns.Members[clientID] = memberMap
				}
			}
		}
	}
	vpnLog("namespace-update", name, "members=%d", len(ns.Members))
}

func (m *VPNManager) HandleNamespaceDeleted(event sdk.Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	name := event.Resource.Name
	m.deleteInterface("wg-" + name)
	delete(m.ns, name)
	vpnLog("namespace-delete", name, "")
}

func (m *VPNManager) CreateNamespace(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.ns[name]; exists {
		return
	}
	m.ns[name] = &namespaceState{Name: name, Members: make(map[string]map[string]any)}
	m.createInterface("wg-"+name, name)
	vpnLog("namespace-create", name, "")
}

func (m *VPNManager) DeleteNamespace(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteInterface("wg-" + name)
	delete(m.ns, name)
	vpnLog("namespace-delete", name, "")
}

func (m *VPNManager) GetInterfaces() []map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]map[string]any, 0, len(m.ifaces))
	for _, iface := range m.ifaces {
		result = append(result, map[string]any{
			"name":      iface.Name,
			"namespace": iface.Namespace,
			"ip":        iface.IP,
			"peers":     len(iface.Peers),
		})
	}
	return result
}

func (m *VPNManager) AddPeer(namespace, clientID, publicKey, endpoint, allowedIPs string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ifaceName := "wg-" + namespace
	iface, exists := m.ifaces[ifaceName]
	if !exists {
		vpnLog("error", ifaceName, "接口不存在")
		return
	}

	// 添加 peer
	peer := &peerState{
		ClientID:   clientID,
		PublicKey:  publicKey,
		Endpoint:   endpoint,
		AllowedIPs: []string{allowedIPs},
	}
	iface.Peers[clientID] = peer

	// 执行 wg set 命令
	args := []string{"set", ifaceName, "peer", publicKey}
	if allowedIPs != "" {
		args = append(args, "allowed-ips", allowedIPs)
	}
	if endpoint != "" {
		args = append(args, "endpoint", endpoint)
	}
	m.exec("wg", args...)

	vpnLog("add-peer", namespace, "client=%s", clientID)
}

func (m *VPNManager) RemovePeer(namespace, clientID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ifaceName := "wg-" + namespace
	iface, exists := m.ifaces[ifaceName]
	if !exists {
		vpnLog("error", ifaceName, "接口不存在")
		return
	}

	peer, exists := iface.Peers[clientID]
	if !exists {
		vpnLog("error", ifaceName, "peer 不存在: %s", clientID)
		return
	}

	// 执行 wg set 命令移除 peer
	m.exec("wg", "set", ifaceName, "peer", peer.PublicKey, "remove")
	delete(iface.Peers, clientID)

	vpnLog("remove-peer", namespace, "client=%s", clientID)
}

func (m *VPNManager) HandleProxyAdded(event sdk.Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	vpnLog("proxy-add", event.Resource.Name, "")
}

func (m *VPNManager) HandleProxyDeleted(event sdk.Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	vpnLog("proxy-delete", event.Resource.Name, "")
}

func (m *VPNManager) HandleRelayAdded(event sdk.Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	vpnLog("relay-add", event.Resource.Name, "")
}

func (m *VPNManager) HandleRelayDeleted(event sdk.Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	vpnLog("relay-delete", event.Resource.Name, "")
}

func (m *VPNManager) createInterface(iface, namespace string) {
	vpnLog("create", iface, "namespace=%s", namespace)
	if !m.exec("ip", "link", "add", iface, "type", "wireguard") {
		vpnLog("error", iface, "创建接口失败")
		return
	}
	ip := allocateIP(namespace)
	if !m.exec("ip", "addr", "add", ip, "dev", iface) {
		vpnLog("error", iface, "分配 IP 失败")
		m.exec("ip", "link", "del", iface)
		return
	}
	if !m.exec("ip", "link", "set", iface, "up") {
		vpnLog("error", iface, "启用接口失败")
		m.exec("ip", "link", "del", iface)
		return
	}
	m.ifaces[iface] = &ifaceState{Name: iface, Namespace: namespace, IP: ip, Peers: make(map[string]*peerState)}
	vpnLog("assign-ip", iface, "ip=%s", ip)
}

func (m *VPNManager) deleteInterface(iface string) {
	vpnLog("delete", iface, "")
	m.exec("ip", "link", "del", iface)
	delete(m.ifaces, iface)
}

func (m *VPNManager) exec(name string, args ...string) bool {
	if m.dry {
		fmt.Printf("  [dry-run] %s %s\n", name, strings.Join(args, " "))
		return true
	}
	cmd := exec.Command(name, args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		fmt.Printf("  [error] %s %s: %v\n%s\n", name, strings.Join(args, " "), err, out)
		return false
	}
	return true
}

func allocateIP(namespace string) string {
	hash := 0
	for _, c := range namespace {
		hash = hash*31 + int(c)
	}
	octet := (hash % 254) + 1
	return fmt.Sprintf("10.0.%d.1/24", octet)
}

func vpnLog(action, name, format string, args ...any) {
	prefix := fmt.Sprintf("[vpn] %-16s %-16s", action, name)
	if format != "" {
		fmt.Printf(prefix+" "+format+"\n", args...)
	} else {
		fmt.Println(prefix)
	}
}
