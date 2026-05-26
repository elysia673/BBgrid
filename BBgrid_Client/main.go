// Package main 是 BBgrid Client 入口
package main

import (
	"BBgrid/common/config"
	alog "BBgrid/common/log"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

// 版本信息，在编译时通过 -ldflags 注入
var (
	Version   = "dev"
	BuildTime = "unknown"
	GitCommit = "unknown"
)

// printVersion 打印客户端版本信息并退出
func printVersion() {
	fmt.Printf("BBgrid Client %s (%s) %s\n", Version, GitCommit, BuildTime)
}

// main 程序主入口
func main() {
	// 命令行参数解析
	configPath := flag.String("config", "client.json", "path to config file")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	// 如果指定了 -version 参数，打印版本后退出
	if *showVersion {
		printVersion()
		return
	}

	// 加载客户端配置文件
	cfg, err := config.LoadClient(*configPath)
	if err != nil {
		alog.Fatal(alog.CatConfig, "load config failed", "error", err)
	}

	// 初始化日志文件
	if cfg.LogPath != "" {
		if err := alog.SetFile(cfg.LogPath); err != nil {
			alog.Fatal(alog.CatConfig, "init log file failed", "error", err, "path", cfg.LogPath)
		}
		alog.Info(alog.CatConfig, "log file enabled", "path", cfg.LogPath)
	}

	// HTTP/WS 协议切换：如果启用 HTTP 模式，将 wss 替换为 ws
	if cfg.UseHTTP {
		cfg.ServerURL = strings.Replace(cfg.ServerURL, "wss://", "ws://", 1)
	}

	// 创建客户端实例，传入所有连接与安全参数
	client := NewClient(cfg.ServerURL, cfg.ClientID, cfg.ClientToken, cfg.PrivateKeyPath, cfg.PublicKeyPath, cfg.CertificatePath, cfg.UseHTTP, cfg.Insecure, cfg.TLSSNI, cfg.Origin, cfg.UDPTunnelKey, time.Duration(cfg.ReconnectDelaySeconds)*time.Second)

	// 注册系统信号处理，实现优雅关闭
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		alog.Info(alog.CatSystem, "收到关闭信号")
		client.Stop()
	}()

	// 启动客户端主循环，阻塞运行直到收到停止信号
	client.Run()
	alog.Flush()
}
