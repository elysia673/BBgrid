package main

import (
	"flag"
	"fmt"
	"os"
)

var (
	Version   = "dev"
	BuildTime = "unknown"
	GitCommit = "unknown"
)

// 默认 socket 路径
const defaultSocketPath = "/var/run/bbgrid/daemon.sock"

func main() {
	// 解析参数
	socketPath := flag.String("socket", defaultSocketPath, "daemon socket 路径")
	showVersion := flag.Bool("version", false, "显示版本信息")
	flag.Parse()

	if *showVersion {
		fmt.Printf("bbgrid-ctl %s (built: %s, commit: %s)\n", Version, BuildTime, GitCommit)
		return
	}

	args := flag.Args()
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	cmd := args[0]
	cmdArgs := args[1:]

	switch cmd {
	case "start":
		handleStart(*socketPath, cmdArgs)
	case "stop":
		handleStop(*socketPath, cmdArgs)
	case "restart":
		handleRestart(*socketPath, cmdArgs)
	case "status":
		handleStatus(*socketPath, cmdArgs)
	case "update":
		handleUpdate(*socketPath, cmdArgs)
	case "rollback":
		handleRollback(*socketPath, cmdArgs)
	case "init":
		handleInit(cmdArgs)
	case "help", "--help", "-h":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`BBgrid Control - 进程管理和自动更新工具

Usage:
  bbgrid-ctl [options] <command> [target]

Options:
  -socket <path>   daemon socket 路径 (默认: /var/run/bbgrid/daemon.sock)
  -version         显示版本信息

Commands:
  start <target>     启动服务 (server|client|all)
  stop <target>      停止服务 (server|client|all)
  restart <target>   重启服务 (server|client|all)
  status             查看所有服务状态
  update <target>    更新服务 (server|client|all)
  rollback <target>  回滚服务 (server|client|all)
  init <target>      生成配置文件 (server|client)

Examples:
  bbgrid-ctl start server
  bbgrid-ctl -socket /var/run/bbgrid/daemon.sock status
  bbgrid-ctl stop all`)
}
