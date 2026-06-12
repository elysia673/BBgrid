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

// 命令行标志定义：
// - server:   BBgrid gRPC Server 的监听地址
// - id:       本 Runtime 实例的唯一标识符
// - dry-run:  试运行模式，仅打印操作而不实际执行 WireGuard 命令
var (
	serverAddr = flag.String("server", "localhost:9910", "BBgrid gRPC server address")
	runtimeID  = flag.String("id", "runtime-1", "Runtime identifier")
	dryRun     = flag.Bool("dry-run", false, "Print operations without executing")
)

func main() {
	flag.Parse()

	// 创建 VPN 管理器，负责底层 WireGuard 接口的创建、删除、Peer 管理等操作。
	// dry-run 模式下仅打印命令，不实际执行。
	vpn := NewVPNManager(*dryRun)
	defer vpn.Close()

	// 创建 SDK 插件客户端，传入 Runtime ID、版本号和 Server 地址，
	// 用于与 BBgrid Server 建立 gRPC 连接并注册自身。
	p := sdk.NewPlugin(*runtimeID, "1.0.0", *serverAddr)

	// 订阅 namespace 事件：当 Server 端命名空间发生变化时收到通知。
	// ADDED   - 新命名空间创建，需初始化对应的 WireGuard 接口
	// MODIFIED - 命名空间配置变更，需同步更新
	// DELETED  - 命名空间删除，需清理相关接口
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

	// 订阅 proxy 事件：当端口代理发生变化时收到通知。
	// ADDED   - 新代理创建，需建立端口转发
	// DELETED  - 代理删除，需关闭转发
	p.On("proxy", func(event sdk.Event) {
		switch event.EventType {
		case "ADDED":
			vpn.HandleProxyAdded(event)
		case "DELETED":
			vpn.HandleProxyDeleted(event)
		}
	})

	// 订阅 relay 事件：当中继会话发生变化时收到通知。
	// ADDED   - 新中继建立，需配置中继转发
	// DELETED  - 中继关闭，需清理配置
	p.On("relay", func(event sdk.Event) {
		switch event.EventType {
		case "ADDED":
			vpn.HandleRelayAdded(event)
		case "DELETED":
			vpn.HandleRelayDeleted(event)
		}
	})

	// 注册 Actions：向 Server 声明本 Runtime 支持的可调用操作，
	// 由 Server 端通过 run 命令远程调用。

	// vpn.status - 查询当前 VPN 整体状态
	p.HandleAction("vpn.status", "查看 VPN 状态", func(params map[string]any) (any, error) {
		return vpn.GetStatus(), nil
	})

	// vpn.create - 创建新的 VPN 命名空间（WireGuard 接口）
	p.HandleAction("vpn.create", "创建 VPN 网络", func(params map[string]any) (any, error) {
		name, _ := params["namespace"].(string)
		if name == "" {
			return nil, fmt.Errorf("namespace is required")
		}
		vpn.CreateNamespace(name)
		return map[string]any{"namespace": name, "status": "created"}, nil
	})

	// vpn.delete - 删除 VPN 命名空间并清理相关资源
	p.HandleAction("vpn.delete", "删除 VPN 网络", func(params map[string]any) (any, error) {
		name, _ := params["namespace"].(string)
		if name == "" {
			return nil, fmt.Errorf("namespace is required")
		}
		vpn.DeleteNamespace(name)
		return map[string]any{"namespace": name, "status": "deleted"}, nil
	})

	// vpn.join - 将远端 Peer 加入指定命名空间的 WireGuard 网络
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

	// vpn.leave - 将 Peer 从指定命名空间的 WireGuard 网络中移除
	p.HandleAction("vpn.leave", "离开 VPN 网络", func(params map[string]any) (any, error) {
		ns, _ := params["namespace"].(string)
		clientID, _ := params["client_id"].(string)
		if ns == "" || clientID == "" {
			return nil, fmt.Errorf("namespace and client_id are required")
		}
		vpn.RemovePeer(ns, clientID)
		return map[string]any{"namespace": ns, "client_id": clientID, "status": "left"}, nil
	})

	// vpn.interfaces - 列出所有 WireGuard 接口及其 Peer 信息
	p.HandleAction("vpn.interfaces", "查看 WireGuard 接口", func(params map[string]any) (any, error) {
		return vpn.GetInterfaces(), nil
	})

	// 优雅退出：监听 SIGINT/SIGTERM 信号，收到后停止插件并清理资源。
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Println("\n正在清理...")
		p.Stop()
	}()

	// 启动插件主循环，阻塞直到 Stop() 被调用或连接断开。
	log.Printf("Runtime %s 启动，连接 %s", *runtimeID, *serverAddr)
	if err := p.Run(); err != nil {
		log.Fatalf("运行失败: %v", err)
	}
}
