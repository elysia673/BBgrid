// BBgrid Runtime
//
// 外部插件：通过通用插件协议连接 Server，管理 WireGuard 接口。
// 使用 SDK 连接 Server，订阅事件，实现 VPN 拓扑管理。
package main

import (
	"BBgrid/common/sdk"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var (
	serverAddr = flag.String("server", "localhost:9910", "BBgrid gRPC server address")
	runtimeID  = flag.String("id", "runtime-1", "Runtime identifier")
	dryRun     = flag.Bool("dry-run", false, "Print operations without executing")
)

func main() {
	flag.Parse()

	// 创建 VPN 管理器
	vpn := NewVPNManager(*dryRun)
	defer vpn.Close()

	// 创建 SDK 插件客户端
	p := sdk.NewPlugin(*runtimeID, "1.0.0", *serverAddr)

	// 订阅 namespace 事件
	p.On("namespace", func(event sdk.Event) {
		switch event.EventType {
		case "ADDED":
			vpn.HandleNamespaceAdded(event)
		case "MODIFIED":
			vpn.HandleNamespaceModified(event)
		case "DELETED":
			vpn.HandleNamespaceDeleted(event)
		}
	})

	// 订阅 proxy 事件
	p.On("proxy", func(event sdk.Event) {
		switch event.EventType {
		case "ADDED":
			vpn.HandleProxyAdded(event)
		case "DELETED":
			vpn.HandleProxyDeleted(event)
		}
	})

	// 订阅 relay 事件
	p.On("relay", func(event sdk.Event) {
		switch event.EventType {
		case "ADDED":
			vpn.HandleRelayAdded(event)
		case "DELETED":
			vpn.HandleRelayDeleted(event)
		}
	})

	// 注册 Actions
	p.HandleAction("vpn.status", "查看 VPN 状态", func(params map[string]any) (any, error) {
		return vpn.GetStatus(), nil
	})

	p.HandleAction("vpn.create", "创建 VPN 网络", func(params map[string]any) (any, error) {
		name, _ := params["namespace"].(string)
		if name == "" {
			return nil, fmt.Errorf("namespace is required")
		}
		vpn.CreateNamespace(name)
		return map[string]any{"namespace": name, "status": "created"}, nil
	})

	p.HandleAction("vpn.delete", "删除 VPN 网络", func(params map[string]any) (any, error) {
		name, _ := params["namespace"].(string)
		if name == "" {
			return nil, fmt.Errorf("namespace is required")
		}
		vpn.DeleteNamespace(name)
		return map[string]any{"namespace": name, "status": "deleted"}, nil
	})

	p.HandleAction("vpn.join", "加入 VPN 网络", func(params map[string]any) (any, error) {
		ns, _ := params["namespace"].(string)
		clientID, _ := params["client_id"].(string)
		if ns == "" || clientID == "" {
			return nil, fmt.Errorf("namespace and client_id are required")
		}
		publicKey, _ := params["public_key"].(string)
		endpoint, _ := params["endpoint"].(string)
		allowedIPs, _ := params["allowed_ips"].(string)
		vpn.AddPeer(ns, clientID, publicKey, endpoint, allowedIPs)
		return map[string]any{"namespace": ns, "client_id": clientID, "status": "joined"}, nil
	})

	p.HandleAction("vpn.leave", "离开 VPN 网络", func(params map[string]any) (any, error) {
		ns, _ := params["namespace"].(string)
		clientID, _ := params["client_id"].(string)
		if ns == "" || clientID == "" {
			return nil, fmt.Errorf("namespace and client_id are required")
		}
		vpn.RemovePeer(ns, clientID)
		return map[string]any{"namespace": ns, "client_id": clientID, "status": "left"}, nil
	})

	p.HandleAction("vpn.interfaces", "查看 WireGuard 接口", func(params map[string]any) (any, error) {
		return vpn.GetInterfaces(), nil
	})

	// 优雅退出
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Println("\n正在清理...")
		p.Stop()
	}()

	log.Printf("Runtime %s 启动，连接 %s", *runtimeID, *serverAddr)
	if err := p.Run(); err != nil {
		log.Fatalf("运行失败: %v", err)
	}
}
