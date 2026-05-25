// Package main 是 BBgrid CLI 工具的入口。
// 提供登录、节点管理、代理管理、中继管理、命名空间管理、注册审核等功能，
// 通过 HTTP REST API 与 BBgrid Server 通信。
package main

import (
	"bytes"
	"crypto/md5"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"
)

// Version / BuildTime / GitCommit 在编译时通过 -ldflags 注入，用于版本信息展示。
var (
	Version   = "dev"
	BuildTime = "unknown"
	GitCommit = "unknown"
)

// CLIConfig 表示 CLI 本地持久化的配置结构。
// 登录成功后会写入 ~/.aether_config.json。
type CLIConfig struct {
	Server   string `json:"server"`    // 服务器地址
	APIKey   string `json:"api_key"`   // API 密钥（登录前使用）
	Token    string `json:"token"`     // JWT Token（登录后使用）
	TokenExp int64  `json:"token_exp"` // Token 过期的 Unix 时间戳
	Insecure bool   `json:"insecure"`  // 是否跳过 TLS 证书校验
}

// Response 是 BBgrid Server 返回的统一 JSON 响应结构。
type Response struct {
	Code int             `json:"code"`              // 业务状态码，0 表示成功
	Msg  string          `json:"msg"`               // 提示信息
	Data json.RawMessage `json:"data,omitempty"`    // 业务数据（延迟解析）
}

// 全局变量：命令行参数解析结果、配置实例、HTTP 客户端。
var (
	configPath  string     // 配置文件路径
	jsonOutput  bool       // 是否以 JSON 格式输出
	showVersion bool       // 是否显示版本信息
	insecure    bool       // 命令行传入的 -insecure 标志
	cfg         *CLIConfig // 当前加载的配置
	httpClient  *http.Client
)

// init 注册全局命令行参数。
func init() {
	home := getHomeDir()
	flag.StringVar(&configPath, "config", filepath.Join(home, ".aether_config.json"), "配置文件路径")
	flag.BoolVar(&jsonOutput, "json", false, "JSON 输出模式")
	flag.BoolVar(&showVersion, "version", false, "版本")
	flag.BoolVar(&insecure, "insecure", false, "跳过 TLS 验证")
	flag.Usage = func() { printMainHelp() }
}

// main 是程序入口，解析全局标志后根据子命令分发到对应的处理函数。
func main() {
	flag.Parse()

	// 显示版本后直接退出
	if showVersion {
		fmt.Printf("bbgrid-cli %s (%s) %s\n", Version, GitCommit, BuildTime)
		return
	}

	args := flag.Args()
	if len(args) == 0 {
		printMainHelp()
	}

	// 加载本地配置并初始化 HTTP 客户端
	var err error
	cfg, err = loadConfig(configPath)
	if err != nil {
		fatal("加载配置失败: %v", err)
	}
	initHTTP()

	// 根据第一个参数（子命令）分发处理
	cmd, rest := args[0], args[1:]
	switch cmd {
	case "login":
		cmdLogin(rest)
	case "ping":
		cmdPing()
	case "status":
		cmdStatus()
	case "node":
		cmdNode(rest)
	case "proxy":
		cmdProxy(rest)
	case "relay":
		cmdRelay(rest)
	case "register":
		cmdRegister(rest)
	case "namespace":
		cmdNamespace(rest)
	case "sync":
		cmdSync()
	case "run":
		cmdRun(rest)
	case "task":
		cmdTask(rest)
	case "update":
		cmdUpdate(rest)
	case "help":
		if len(rest) > 0 {
			printCommandHelp(rest[0])
		} else {
			printMainHelp()
		}
	default:
		fmt.Fprintf(os.Stderr, "未知命令: %s\n\n", cmd)
		printMainHelp()
	}
}

// ==================== 帮助 ====================

// printMainHelp 输出主帮助信息并退出。
func printMainHelp() {
	fmt.Print(`bbgrid-cli - BBgrid 网络代理管理工具

用法: bbgrid-cli [全局选项] <命令> [参数]

命令:
  login       登录服务器
  ping        健康检查
  status      服务器状态
  node        节点管理
  proxy       代理管理
  relay       中继管理
  register    客户端注册
  namespace   命名空间管理
  sync        同步可用操作
  run         执行操作
  task        查询任务状态
  update      更新二进制

全局选项:
  -config <path>    配置文件 (默认 ~/.aether_config.json)
  -json             JSON 输出
  -insecure         跳过 TLS 验证
  -version          版本

帮助:
  bbgrid-cli help <命令>    查看命令详细用法
`)
	os.Exit(0)
}

// printCommandHelp 根据子命令名称输出对应帮助信息并退出。
func printCommandHelp(cmd string) {
	helps := map[string]string{
		"login": `用法: bbgrid-cli login -server <url> -api-key <key>

选项:
  -server <url>     服务器地址 (必填)
  -api-key <key>    API 密钥 (必填)`,

		"node": `用法: bbgrid-cli node <子命令> [参数]

子命令:
  list                列出所有节点
  info <client-id>    查看节点详情`,

		"proxy": `用法: bbgrid-cli proxy <子命令> [参数]

子命令:
  list                          列出所有代理
  create <client-id> [选项]     创建代理
  close <port>                  关闭代理

create 选项:
  -remote <port>    服务端端口 (必填)
  -local <port>     客户端端口 (必填)
  -protocol <tcp|udp>  协议 (默认 tcp)
  -bind <addr>      绑定地址 (默认 0.0.0.0)`,

		"relay": `用法: bbgrid-cli relay [子命令] [参数]

子命令:
  create <source> <target> [选项]   创建中继
  list                              列出中继会话
  close <session-id>                关闭中继

create 选项:
  -source-port <port>   源端口 (必填)
  -target-port <port>   目标端口 (必填)
  -protocol <tcp|udp>   协议 (默认 tcp)`,

		"register": `用法: bbgrid-cli register <子命令> [参数]

子命令:
  apply     提交注册申请
  approve   审核通过
  revoke    吊销客户端
  pending   查看待审核列表
  list      查看已通过列表`,

		"namespace": `用法: bbgrid-cli namespace <子命令> [参数]

子命令:
  list                              列出所有命名空间
  info <name>                       命名空间详情
  clients <name>                    命名空间下的客户端
  assign <client-id> <ns> <role>    分配客户端`,

		"sync": `用法: bbgrid-cli sync

同步服务器可用操作，结果缓存到 ~/.aether_sync.json`,

		"run": `用法: bbgrid-cli run <action> [key=value...]

执行操作（异步）。
参数以 key=value 形式传入。

示例:
  bbgrid-cli run latency.get client_id=node-01
  bbgrid-cli run tag.set client_id=node-01 key=env value=prod`,

		"task": `用法: bbgrid-cli task <task-id>

查询异步任务状态`,

		"update": `用法: bbgrid-cli update <子命令> [参数]

子命令:
  server <binary>                 更新服务端
  client -f <binary> -target <id> 更新客户端`,
	}

	if h, ok := helps[cmd]; ok {
		fmt.Println(h)
	} else {
		fmt.Fprintf(os.Stderr, "没有关于 '%s' 的帮助\n", cmd)
	}
	os.Exit(0)
}

// ==================== 命令实现 ====================

// cmdLogin 处理登录命令。
// 使用 API Key 向服务器换取 JWT Token，成功后将配置写入本地。
func cmdLogin(args []string) {
	fs := flag.NewFlagSet("login", flag.ExitOnError)
	server := fs.String("server", "", "服务器地址")
	apiKey := fs.String("api-key", "", "API 密钥")
	fs.Parse(args)

	if *server == "" || *apiKey == "" {
		fmt.Fprintln(os.Stderr, "错误: -server 和 -api-key 必填")
		fmt.Fprintln(os.Stderr, "用法: bbgrid-cli login -server <url> -api-key <key>")
		os.Exit(1)
	}

	// 发送登录请求
	resp, err := httpClient.Post(*server+"/api/v1/auth/login", "application/json",
		bytes.NewReader([]byte(fmt.Sprintf(`{"api_key":"%s"}`, *apiKey))))
	if err != nil {
		fatal("请求失败: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		fatal("登录失败 (HTTP %d): %s", resp.StatusCode, string(body))
	}

	// 解析登录响应
	var result struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			Token     string `json:"token"`
			ExpiresIn int64  `json:"expires_in"`
		} `json:"data"`
	}
	json.Unmarshal(body, &result)
	if result.Code != 0 {
		fatal("登录失败: %s", result.Msg)
	}

	// 更新配置并持久化
	cfg.Server = strings.TrimRight(*server, "/")
	cfg.Token = result.Data.Token
	cfg.TokenExp = time.Now().Unix() + result.Data.ExpiresIn
	cfg.Insecure = cfg.Insecure || insecure

	data, _ := json.MarshalIndent(cfg, "", "  ")
	os.WriteFile(configPath, data, 0600)
	fmt.Println("登录成功")
}

// cmdPing 执行健康检查，向服务器发送 GET /PING 请求。
func cmdPing() {
	resp, err := api("GET", "/PING", nil)
	fatalOn(err)
	print(resp)
}

// cmdStatus 查询服务器状态。
func cmdStatus() {
	resp, err := api("GET", "/status", nil)
	fatalOn(err)
	print(resp)
}

// cmdNode 处理节点管理子命令：list / info。
func cmdNode(args []string) {
	if len(args) == 0 {
		printCommandHelp("node")
		os.Exit(1)
	}
	sub, args := args[0], args[1:]

	switch sub {
	case "list":
		// 列出所有节点
		resp, err := api("GET", "/api/v1/nodes", nil)
		fatalOn(err)

		if jsonOutput {
			printJSON(resp)
			return
		}

		// 解析节点列表并以表格形式输出
		var data struct {
			Clients []struct {
				ID         string `json:"id"`
				RemoteAddr string `json:"remote_addr"`
				ProxyCount int    `json:"proxy_count"`
				Host       string `json:"host"`
				Online     bool   `json:"online"`
			} `json:"clients"`
		}
		unmarshal(resp.Data, &data)

		if len(data.Clients) == 0 {
			fmt.Println("没有节点")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\t地址\t状态\t代理数\t主机")
		fmt.Fprintln(w, "--\t----\t----\t------\t----")
		for _, c := range data.Clients {
			status := "离线"
			if c.Online {
				status = "在线"
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\n", c.ID, c.RemoteAddr, status, c.ProxyCount, c.Host)
		}
		w.Flush()

	case "info":
		// 查看指定节点详情
		if len(args) == 0 {
			fmt.Fprintln(os.Stderr, "用法: bbgrid-cli node info <client-id>")
			os.Exit(1)
		}
		resp, err := api("GET", "/api/v1/nodes/"+args[0], nil)
		fatalOn(err)
		print(resp)

	default:
		fmt.Fprintf(os.Stderr, "未知子命令: %s\n", sub)
		fmt.Fprintln(os.Stderr, "用法: bbgrid-cli node <list|info>")
		os.Exit(1)
	}
}

// cmdProxy 处理代理管理子命令：list / create / close。
func cmdProxy(args []string) {
	if len(args) == 0 {
		printCommandHelp("proxy")
		os.Exit(1)
	}
	sub, args := args[0], args[1:]

	switch sub {
	case "list":
		// 列出所有代理
		resp, err := api("GET", "/api/v1/proxies", nil)
		fatalOn(err)
		if jsonOutput {
			printJSON(resp)
			return
		}
		var data struct {
			Proxies []struct {
				ClientID   string `json:"client_id"`
				RemotePort int    `json:"remote_port"`
				LocalPort  int    `json:"local_port"`
				PublicAddr string `json:"public_addr"`
			} `json:"proxies"`
		}
		unmarshal(resp.Data, &data)
		if len(data.Proxies) == 0 {
			fmt.Println("没有代理")
			return
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "客户端\t远程端口\t本地端口\t公网地址")
		fmt.Fprintln(w, "------\t--------\t--------\t--------")
		for _, p := range data.Proxies {
			fmt.Fprintf(w, "%s\t%d\t%d\t%s\n", p.ClientID, p.RemotePort, p.LocalPort, p.PublicAddr)
		}
		w.Flush()

	case "create":
		// 创建端口代理
		if len(args) < 1 {
			fmt.Fprintln(os.Stderr, "用法: bbgrid-cli proxy create <client-id> -remote <port> -local <port>")
			os.Exit(1)
		}
		clientID := args[0]
		fs := flag.NewFlagSet("proxy create", flag.ExitOnError)
		remote := fs.Int("remote", 0, "服务端端口")
		local := fs.Int("local", 0, "客户端端口")
		protocol := fs.String("protocol", "tcp", "协议")
		bind := fs.String("bind", "0.0.0.0", "绑定地址")
		localIP := fs.String("local-ip", "127.0.0.1", "客户端 IP")
		fs.Parse(args[1:])
		if *remote == 0 || *local == 0 {
			fmt.Fprintln(os.Stderr, "错误: -remote 和 -local 必填")
			os.Exit(1)
		}
		resp, err := api("POST", "/api/v1/proxies", map[string]any{
			"client_id": clientID, "remote_port": *remote, "local_port": *local,
			"protocol": *protocol, "bind_addr": *bind, "local_ip": *localIP,
		})
		fatalOn(err)
		print(resp)

	case "close":
		// 关闭指定端口的代理
		if len(args) < 1 {
			fmt.Fprintln(os.Stderr, "用法: bbgrid-cli proxy close <port>")
			os.Exit(1)
		}
		resp, err := api("DELETE", "/api/v1/proxies/"+args[0], nil)
		fatalOn(err)
		print(resp)

	default:
		fmt.Fprintf(os.Stderr, "未知子命令: %s\n", sub)
		fmt.Fprintln(os.Stderr, "用法: bbgrid-cli proxy <list|create|close>")
		os.Exit(1)
	}
}

// cmdRelay 处理中继管理子命令：list / create / close。
func cmdRelay(args []string) {
	sub := "list"
	if len(args) > 0 {
		sub = args[0]
		args = args[1:]
	}

	switch sub {
	case "list":
		// 列出所有中继会话
		resp, err := api("GET", "/api/v1/relay", nil)
		fatalOn(err)
		if jsonOutput {
			printJSON(resp)
			return
		}
		var data struct {
			Sessions []struct {
				SessionID    string `json:"session_id"`
				SourceClient string `json:"source_client"`
				TargetClient string `json:"target_client"`
				Protocol     string `json:"protocol"`
				Status       string `json:"status"`
			} `json:"sessions"`
		}
		unmarshal(resp.Data, &data)
		if len(data.Sessions) == 0 {
			fmt.Println("没有中继会话")
			return
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "会话ID\t源端\t目标\t协议\t状态")
		fmt.Fprintln(w, "-------\t----\t----\t----\t----")
		for _, s := range data.Sessions {
			id := s.SessionID
			if len(id) > 12 {
				id = id[:12]
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", id, s.SourceClient, s.TargetClient, s.Protocol, s.Status)
		}
		w.Flush()

	case "create":
		// 创建中继：在源客户端和目标客户端之间建立端口转发
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "用法: bbgrid-cli relay create <source> <target> -source-port <port> -target-port <port>")
			os.Exit(1)
		}
		source, target := args[0], args[1]
		fs := flag.NewFlagSet("relay create", flag.ExitOnError)
		sourcePort := fs.Int("source-port", 0, "源端口")
		targetPort := fs.Int("target-port", 0, "目标端口")
		protocol := fs.String("protocol", "tcp", "协议")
		targetIP := fs.String("target-ip", "127.0.0.1", "目标 IP")
		sourceIP := fs.String("source-ip", "0.0.0.0", "源 IP")
		fs.Parse(args[2:])
		if *sourcePort == 0 || *targetPort == 0 {
			fmt.Fprintln(os.Stderr, "错误: -source-port 和 -target-port 必填")
			os.Exit(1)
		}
		resp, err := api("POST", "/api/v1/relay", map[string]any{
			"source_client_id": source, "target_client_id": target,
			"source_port": *sourcePort, "target_port": *targetPort,
			"protocol": *protocol, "target_local_ip": *targetIP, "source_local_ip": *sourceIP,
		})
		fatalOn(err)
		print(resp)

	case "close":
		// 关闭指定中继会话
		if len(args) < 1 {
			fmt.Fprintln(os.Stderr, "用法: bbgrid-cli relay close <session-id>")
			os.Exit(1)
		}
		resp, err := api("DELETE", "/api/v1/relay/"+args[0], nil)
		fatalOn(err)
		print(resp)

	default:
		fmt.Fprintf(os.Stderr, "未知子命令: %s\n", sub)
		fmt.Fprintln(os.Stderr, "用法: bbgrid-cli relay <list|create|close>")
		os.Exit(1)
	}
}

// cmdRegister 处理客户端注册审核流程：apply / approve / revoke / pending / list。
func cmdRegister(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "用法: bbgrid-cli register <apply|approve|revoke|pending|list>")
		os.Exit(1)
	}
	sub, rest := args[0], args[1:]

	switch sub {
	case "apply":
		// 提交注册申请：上传客户端 ID、公钥和认证 Token
		fs := flag.NewFlagSet("register apply", flag.ExitOnError)
		id := fs.String("id", "", "客户端 ID")
		pubkey := fs.String("pubkey", "", "公钥文件")
		token := fs.String("token", "", "认证 token")
		fs.Parse(rest)
		if *id == "" || *pubkey == "" || *token == "" {
			fmt.Fprintln(os.Stderr, "错误: -id, -pubkey, -token 必填")
			os.Exit(1)
		}
		keyData, err := os.ReadFile(*pubkey)
		fatalOn(err)
		resp, err := api("POST", "/api/v1/register/apply", map[string]string{
			"client_id": *id, "public_key": string(keyData), "token": *token,
		})
		fatalOn(err)
		print(resp)

	case "approve":
		// 审核通过：为客户端生成证书并保存到本地文件
		fs := flag.NewFlagSet("register approve", flag.ExitOnError)
		id := fs.String("id", "", "客户端 ID")
		ns := fs.String("namespace", "permanent", "命名空间")
		role := fs.String("role", "permanent", "角色")
		fs.Parse(rest)
		if *id == "" {
			fmt.Fprintln(os.Stderr, "错误: -id 必填")
			os.Exit(1)
		}
		resp, err := api("POST", "/api/v1/register/approve", map[string]string{
			"client_id": *id, "namespace": *ns, "role": *role,
		})
		fatalOn(err)
		if jsonOutput {
			printJSON(resp)
			return
		}
		var data struct {
			Certificate string `json:"certificate"`
			ClientID    string `json:"client_id"`
		}
		unmarshal(resp.Data, &data)
		outFile := *id + ".crt"
		os.WriteFile(outFile, []byte(data.Certificate), 0600)
		fmt.Printf("审核通过，证书已保存至: %s\n", outFile)

	case "revoke":
		// 吊销客户端证书
		fs := flag.NewFlagSet("register revoke", flag.ExitOnError)
		id := fs.String("id", "", "客户端 ID")
		fs.Parse(rest)
		if *id == "" {
			fmt.Fprintln(os.Stderr, "错误: -id 必填")
			os.Exit(1)
		}
		resp, err := api("POST", "/api/v1/register/revoke", map[string]string{"client_id": *id})
		fatalOn(err)
		print(resp)

	case "pending":
		// 查看待审核列表
		resp, err := api("GET", "/api/v1/register/pending", nil)
		fatalOn(err)
		print(resp)

	case "list":
		// 查看已通过注册的客户端列表
		resp, err := api("GET", "/api/v1/register/list", nil)
		fatalOn(err)
		print(resp)

	default:
		fmt.Fprintf(os.Stderr, "未知子命令: %s\n", sub)
		fmt.Fprintln(os.Stderr, "用法: bbgrid-cli register <apply|approve|revoke|pending|list>")
		os.Exit(1)
	}
}

// cmdNamespace 处理命名空间管理子命令：list / info / clients / assign。
func cmdNamespace(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "用法: bbgrid-cli namespace <list|info|clients|assign>")
		os.Exit(1)
	}
	sub, rest := args[0], args[1:]

	switch sub {
	case "list":
		// 列出所有命名空间
		resp, err := api("GET", "/api/v1/namespaces", nil)
		fatalOn(err)
		print(resp)

	case "info":
		// 查看指定命名空间详情
		if len(rest) < 1 {
			fmt.Fprintln(os.Stderr, "用法: bbgrid-cli namespace info <name>")
			os.Exit(1)
		}
		resp, err := api("GET", "/api/v1/namespaces/"+rest[0], nil)
		fatalOn(err)
		print(resp)

	case "clients":
		// 查看命名空间下的客户端列表
		if len(rest) < 1 {
			fmt.Fprintln(os.Stderr, "用法: bbgrid-cli namespace clients <name>")
			os.Exit(1)
		}
		resp, err := api("GET", "/api/v1/namespaces/"+rest[0]+"/clients", nil)
		fatalOn(err)
		print(resp)

	case "assign":
		// 将客户端分配到指定命名空间并赋予角色
		if len(rest) < 3 {
			fmt.Fprintln(os.Stderr, "用法: bbgrid-cli namespace assign <client-id> <namespace> <role>")
			os.Exit(1)
		}
		resp, err := api("POST", "/api/v1/namespaces/assign", map[string]string{
			"client_id": rest[0], "namespace": rest[1], "role": rest[2],
		})
		fatalOn(err)
		print(resp)

	default:
		fmt.Fprintf(os.Stderr, "未知子命令: %s\n", sub)
		fmt.Fprintln(os.Stderr, "用法: bbgrid-cli namespace <list|info|clients|assign>")
		os.Exit(1)
	}
}

// cmdSync 从服务器同步可用的操作列表并缓存到 ~/.aether_sync.json。
func cmdSync() {
	resp, err := api("GET", "/api/v1/sync", nil)
	fatalOn(err)

	// 将同步数据写入本地缓存
	home := getHomeDir()
	syncFile := filepath.Join(home, ".aether_sync.json")
	os.WriteFile(syncFile, resp.Data, 0644)

	if jsonOutput {
		printJSON(resp)
		return
	}

	// 解析并按插件分组展示可用操作
	var data struct {
		Plugins []struct {
			PluginID string `json:"plugin_id"`
			Actions  []struct {
				Name        string `json:"name"`
				Description string `json:"description,omitempty"`
			} `json:"actions"`
		} `json:"plugins"`
	}
	unmarshal(resp.Data, &data)

	if len(data.Plugins) == 0 {
		fmt.Println("没有可用操作")
		return
	}

	for _, p := range data.Plugins {
		fmt.Printf("[%s]\n", p.PluginID)
		for _, a := range p.Actions {
			desc := a.Description
			if desc == "" {
				desc = "-"
			}
			fmt.Printf("  %-20s %s\n", a.Name, desc)
		}
	}
}

// cmdRun 执行远程操作（异步），参数以 key=value 形式传入。
func cmdRun(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "用法: bbgrid-cli run <action> [key=value...]")
		os.Exit(1)
	}

	action := args[0]
	// 解析 key=value 参数
	params := make(map[string]any)
	for _, arg := range args[1:] {
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) == 2 {
			params[parts[0]] = parts[1]
		}
	}

	resp, err := api("POST", "/api/v1/run", map[string]any{
		"action": action, "params": params,
	})
	if err != nil {
		// 操作未找到时，展示可用操作列表辅助排查
		if strings.Contains(err.Error(), "action not found") {
			showActions(action)
		}
		fatal("请求失败: %v", err)
	}

	if jsonOutput {
		printJSON(resp)
		return
	}

	var data map[string]any
	unmarshal(resp.Data, &data)
	delete(data, "task_id")
	printData(data)
}

// cmdTask 根据任务 ID 查询异步任务的执行状态。
func cmdTask(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "用法: bbgrid-cli task <task-id>")
		os.Exit(1)
	}
	resp, err := api("GET", "/api/v1/tasks/"+args[0], nil)
	fatalOn(err)
	print(resp)
}

// cmdUpdate 处理二进制更新子命令：server（更新服务端）/ client（更新客户端）。
func cmdUpdate(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "用法: bbgrid-cli update <server|client>")
		os.Exit(1)
	}
	sub, rest := args[0], args[1:]

	switch sub {
	case "server":
		// 上传二进制文件更新服务端
		if len(rest) < 1 {
			fmt.Fprintln(os.Stderr, "用法: bbgrid-cli update server <binary>")
			os.Exit(1)
		}
		md5sum := calcMD5(rest[0])
		uploadBinary(rest[0], cfg.Server+"/api/v1/update", md5sum)
		fmt.Println("服务端更新成功")

	case "client":
		// 上传二进制文件更新客户端，-target all 表示批量更新所有节点
		fs := flag.NewFlagSet("update client", flag.ExitOnError)
		binary := fs.String("f", "", "二进制文件")
		target := fs.String("target", "", "目标 ID 或 all")
		fs.Parse(rest)
		if *binary == "" || *target == "" {
			fmt.Fprintln(os.Stderr, "错误: -f 和 -target 必填")
			os.Exit(1)
		}
		md5sum := calcMD5(*binary)
		if *target == "all" {
			// 获取所有节点并逐一更新
			resp, err := api("GET", "/api/v1/nodes", nil)
			fatalOn(err)
			var data struct {
				Clients []struct {
					ID string `json:"id"`
				} `json:"clients"`
			}
			unmarshal(resp.Data, &data)
			for _, c := range data.Clients {
				fmt.Printf("  更新 %s...\n", c.ID)
				uploadBinary(*binary, cfg.Server+"/api/v1/clients/"+c.ID+"/update", md5sum)
			}
			fmt.Println("全部完成")
		} else {
			// 更新单个指定客户端
			uploadBinary(*binary, cfg.Server+"/api/v1/clients/"+*target+"/update", md5sum)
			fmt.Printf("已发送到 %s\n", *target)
		}

	default:
		fmt.Fprintf(os.Stderr, "未知子命令: %s\n", sub)
		fmt.Fprintln(os.Stderr, "用法: bbgrid-cli update <server|client>")
		os.Exit(1)
	}
}

// ==================== 辅助函数 ====================

// showActions 从本地缓存读取同步数据，展示可用操作列表。
// 当 run 命令找不到对应操作时自动调用，辅助用户排查。
func showActions(input string) {
	home := getHomeDir()
	data, err := os.ReadFile(filepath.Join(home, ".aether_sync.json"))
	if err != nil {
		return
	}
	var syncData struct {
		Plugins []struct {
			PluginID string `json:"plugin_id"`
			Actions  []struct {
				Name        string `json:"name"`
				Description string `json:"description,omitempty"`
			} `json:"actions"`
		} `json:"plugins"`
	}
	if json.Unmarshal(data, &syncData) != nil || len(syncData.Plugins) == 0 {
		return
	}
	fmt.Fprintln(os.Stderr, "\n可用操作:")
	for _, p := range syncData.Plugins {
		fmt.Fprintf(os.Stderr, "  [%s]\n", p.PluginID)
		for _, a := range p.Actions {
			desc := a.Description
			if desc == "" {
				desc = "-"
			}
			fmt.Fprintf(os.Stderr, "    %-20s %s\n", a.Name, desc)
		}
	}
	fmt.Fprintln(os.Stderr)
}

// initHTTP 初始化全局 HTTP 客户端，配置连接池、超时和 TLS 策略。
func initHTTP() {
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	httpClient = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:   &tls.Config{InsecureSkipVerify: cfg.Insecure || insecure},
			DialContext:       dialer.DialContext,
			MaxIdleConns:      10,
			IdleConnTimeout:   90 * time.Second,
			ForceAttemptHTTP2: true,
		},
		Timeout: 10 * time.Second,
	}
}

// api 是所有 REST API 调用的统一入口。
// 自动设置认证头（Bearer Token 或 X-API-KEY）、序列化请求体、解析响应。
func api(method, path string, body interface{}) (*Response, error) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, cfg.Server+path, reader)
	if err != nil {
		return nil, err
	}
	// 优先使用 Token 认证，否则回退到 API Key
	if cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.Token)
	} else {
		req.Header.Set("X-API-KEY", cfg.APIKey)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var result Response
	json.Unmarshal(respBody, &result)
	return &result, nil
}

// getHomeDir 获取当前用户的主目录，失败时返回当前目录 "."。
func getHomeDir() string {
	usr, err := user.Current()
	if err != nil {
		return "."
	}
	return usr.HomeDir
}

// loadConfig 从指定路径加载 CLI 配置文件。
// 文件不存在时返回空配置，不会报错。
func loadConfig(path string) (*CLIConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &CLIConfig{}, nil
		}
		return nil, err
	}
	var cfg CLIConfig
	json.Unmarshal(data, &cfg)
	cfg.Server = strings.TrimRight(cfg.Server, "/")
	return &cfg, nil
}

// unmarshal 是 json.Unmarshal 的便捷封装，用于解析 Response.Data 字段。
func unmarshal(data json.RawMessage, v interface{}) {
	json.Unmarshal(data, v)
}

// print 根据全局 jsonOutput 标志决定输出格式，并处理业务错误码。
func print(resp *Response) {
	if jsonOutput {
		printJSON(resp)
		return
	}
	if resp.Code != 0 {
		fmt.Fprintf(os.Stderr, "错误: %s\n", resp.Msg)
		os.Exit(1)
	}
	if len(resp.Data) > 0 {
		var data interface{}
		unmarshal(resp.Data, &data)
		printData(data)
	} else {
		fmt.Println("成功")
	}
}

// printJSON 将任意值格式化为缩进 JSON 并输出到标准输出。
func printJSON(v interface{}) {
	data, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(data))
}

// printData 根据数据类型分发到对应的格式化输出函数。
func printData(data interface{}) {
	switch v := data.(type) {
	case map[string]interface{}:
		printMap(v)
	case []interface{}:
		printList(v)
	default:
		fmt.Println(v)
	}
}

// printMap 以 key: value 表格形式输出 map 数据，嵌套结构递归展示。
func printMap(m map[string]interface{}) {
	printMapIndent(m, 0)
}

// printMapIndent 递归打印 map，支持嵌套 map 和数组。
func printMapIndent(m map[string]interface{}, depth int) {
	indent := strings.Repeat("  ", depth)
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	for k, v := range m {
		switch val := v.(type) {
		case map[string]interface{}:
			fmt.Fprintf(w, "%s%s:\n", indent, k)
			w.Flush()
			printMapIndent(val, depth+1)
		case []interface{}:
			fmt.Fprintf(w, "%s%s:\n", indent, k)
			w.Flush()
			printListIndent(val, depth+1)
		default:
			fmt.Fprintf(w, "%s%s:\t%v\n", indent, k, v)
		}
	}
	w.Flush()
}

// printListIndent 递归打印数组，支持嵌套结构。
func printListIndent(list []interface{}, depth int) {
	indent := strings.Repeat("  ", depth)
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	for _, item := range list {
		if m, ok := item.(map[string]interface{}); ok {
			fmt.Fprintf(w, "%s-\n", indent)
			w.Flush()
			printMapIndent(m, depth+1)
		} else {
			fmt.Fprintf(w, "%s- %v\n", indent, item)
		}
	}
	w.Flush()
}

// printList 输出数组数据，支持嵌套结构。
func printList(list []interface{}) {
	printListIndent(list, 0)
}

// calcMD5 计算文件的 MD5 校验和，返回十六进制字符串。
func calcMD5(path string) string {
	f, err := os.Open(path)
	fatalOn(err)
	defer f.Close()
	h := md5.New()
	io.Copy(h, f)
	return hex.EncodeToString(h.Sum(nil))
}

// uploadBinary 以 multipart/form-data 方式上传二进制文件和 MD5 校验和到指定 URL。
func uploadBinary(path, url, md5sum string) {
	f, err := os.Open(path)
	fatalOn(err)
	defer f.Close()

	body := &bytes.Buffer{}
	w := multipart.NewWriter(body)
	w.WriteField("md5", md5sum)
	part, _ := w.CreateFormFile("binary", filepath.Base(path))
	io.Copy(part, f)
	w.Close()

	req, _ := http.NewRequest("POST", url, body)
	req.Header.Set("Content-Type", w.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+cfg.Token)

	resp, err := httpClient.Do(req)
	fatalOn(err)
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		fatal("上传失败 (HTTP %d): %s", resp.StatusCode, string(respBody))
	}
}

// fatal 输出格式化错误信息到标准错误并以状态码 1 退出。
func fatal(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(1)
}

// fatalOn 在 err 非 nil 时调用 fatal 终止程序，用于简化错误处理。
func fatalOn(err error) {
	if err != nil {
		fatal("%v", err)
	}
}
