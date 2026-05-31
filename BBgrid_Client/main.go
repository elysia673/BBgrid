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

var (
	Version   = "dev"
	BuildTime = "unknown"
	GitCommit = "unknown"
)

func printVersion() {
	fmt.Printf("Aether Client %s (%s) %s\n", Version, GitCommit, BuildTime)
}

func main() {
	configPath := flag.String("config", "client.json", "path to config file")
	showVersion := flag.Bool("version", false, "print version and exit")
	tempMode := flag.Bool("temp", false, "run as temporary client")
	tempURL := flag.String("temp-url", "", "WebSocket URL for temporary client")
	tempID := flag.String("temp-id", "", "Client ID for temporary client")
	tempToken := flag.String("temp-token", "", "Token for temporary client")
	tempInsecure := flag.Bool("temp-insecure", false, "Skip TLS verification for temporary client")
	flag.Parse()

	if *showVersion {
		printVersion()
		return
	}

	// 临时客户端模式
	if *tempMode {
		if *tempURL == "" || *tempID == "" || *tempToken == "" {
			alog.Fatal(alog.CatConfig, "临时客户端需要 -temp-url, -temp-id, -temp-token 参数")
		}

		client := NewTempClient(*tempURL, *tempID, *tempToken, "", *tempInsecure)

		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sigCh
			alog.Info(alog.CatSystem, "收到关闭信号")
			client.Stop()
		}()

		client.Run()
		alog.Flush()
		return
	}

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

	if cfg.UseHTTP {
		cfg.ServerURL = strings.Replace(cfg.ServerURL, "wss://", "ws://", 1)
	}

	// 创建日志收集器
	var logCollector *LogCollector
	if cfg.Files.APIEnabled {
		logCollector = NewLogCollector(cfg.ClientID, cfg.DataDir, cfg.Files.BatchSizeKB, cfg.Files.BatchIntervalSec)
		logCollector.Start()
	}

	// 启动文件 API 服务器（先创建，再传进 Client）
	var apiServer *FileAPIServer
	if cfg.Files.APIEnabled {
		serverURL := cfg.ServerURL
		if strings.HasPrefix(serverURL, "wss://") {
			serverURL = "https://" + serverURL[6:]
		} else if strings.HasPrefix(serverURL, "ws://") {
			serverURL = "http://" + serverURL[5:]
		}
		serverURL = strings.TrimSuffix(serverURL, "/ws")

		apiServer = NewFileAPIServer(cfg.Files.APIPort, serverURL, cfg.ClientID, cfg.DataDir, cfg.ClientToken)
		if err := apiServer.Start(); err != nil {
			alog.Fatal(alog.CatSystem, "Failed to start file API server", "error", err)
		}
	}

	client := NewClient(cfg.ServerURL, cfg.ClientID, cfg.ClientToken, cfg.PrivateKeyPath, cfg.PublicKeyPath, cfg.CertificatePath, cfg.UseHTTP, cfg.Insecure, cfg.TLSSNI, cfg.Origin, cfg.UDPTunnelKey, cfg.DataDir, time.Duration(cfg.ReconnectDelaySeconds)*time.Second, logCollector)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		alog.Info(alog.CatSystem, "收到关闭信号")
		if apiServer != nil {
			apiServer.Stop()
		}
		if logCollector != nil {
			logCollector.Stop()
		}
		client.Stop()
	}()

	client.Run()
	alog.Flush()
}
