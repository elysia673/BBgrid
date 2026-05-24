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

	client := NewClient(cfg.ServerURL, cfg.ClientID, cfg.ClientToken, cfg.PrivateKeyPath, cfg.PublicKeyPath, cfg.CertificatePath, cfg.UseHTTP, cfg.Insecure, cfg.TLSSNI, cfg.Origin, cfg.UDPTunnelKey, time.Duration(cfg.ReconnectDelaySeconds)*time.Second)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		alog.Info(alog.CatSystem, "收到关闭信号")
		client.Stop()
	}()

	client.Run()
	alog.Flush()
}
