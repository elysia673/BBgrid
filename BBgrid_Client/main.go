package main

import (
	"BBgrid/BBgrid_Client/sdk"
	"BBgrid/common/config"
	alog "BBgrid/common/log"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	Version   = "dev"
	BuildTime = "unknown"
	GitCommit = "unknown"
)

func printVersion() {
	fmt.Printf("BBgrid Client %s (%s) %s\n", Version, GitCommit, BuildTime)
}

func main() {
	configPath := flag.String("config", "client.json", "path to config file")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		printVersion()
		return
	}

	// 加载配置
	cfg, err := config.LoadClient(*configPath)
	if err != nil {
		log.Fatalf("[Config] Load config failed: %v", err)
	}

	// 初始化日志文件
	if cfg.LogPath != "" {
		if err := alog.SetFile(cfg.LogPath); err != nil {
			log.Printf("[Log] Failed to set log file: %v", err)
		} else {
			log.Printf("[Log] Log file enabled: %s", cfg.LogPath)
		}
	}

	log.Printf("[Main] BBgrid Client %s starting...", Version)
	log.Printf("[Main] Server: %s", cfg.ServerURL)
	log.Printf("[Main] Client ID: %s", cfg.ClientID)

	// 创建 SDK
	s := sdk.New(sdk.Config{
		ServerURL:      cfg.ServerURL,
		ClientID:       cfg.ClientID,
		ClientToken:    cfg.ClientToken,
		PrivateKeyPath: cfg.PrivateKeyPath,
		PublicKeyPath:  cfg.PublicKeyPath,
		CertPath:       cfg.CertificatePath,
		DataDir:        cfg.DataDir,
		UseHTTP:        cfg.UseHTTP,
		Insecure:       cfg.Insecure,
		TLSSNI:         cfg.TLSSNI,
		Origin:         cfg.Origin,
		ReconnectDelay: time.Duration(cfg.ReconnectDelaySeconds) * time.Second,
	})

	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 信号处理
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Printf("[Main] Received shutdown signal")
		cancel()
		s.Stop()
	}()

	// 启动 SDK
	if err := s.Start(ctx); err != nil {
		log.Fatalf("[Main] Failed to start SDK: %v", err)
	}

	log.Printf("[Main] Client started, waiting for connections...")

	// 等待上下文取消
	<-ctx.Done()

	log.Printf("[Main] Client shutting down...")
	alog.Flush()
}
