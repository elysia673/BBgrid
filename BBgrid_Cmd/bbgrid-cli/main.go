// Package main 是 BBgrid CLI 工具
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

var (
	Version   = "dev"
	BuildTime = "unknown"
	GitCommit = "unknown"
)

type CLIConfig struct {
	Server   string `json:"server"`
	APIKey   string `json:"api_key"`
	Token    string `json:"token"`
	TokenExp int64  `json:"token_exp"`
	Insecure bool   `json:"insecure"`
}

type Response struct {
	Code int             `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data,omitempty"`
}

var (
	configPath  string
	jsonOutput  bool
	showVersion bool
	insecure    bool
	cfg         *CLIConfig
	httpClient  *http.Client
)

func init() {
	home := getHomeDir()
	flag.StringVar(&configPath, "config", filepath.Join(home, ".aether_config.json"), "配置文件路径")
	flag.BoolVar(&jsonOutput, "json", false, "JSON 输出模式")
	flag.BoolVar(&showVersion, "version", false, "版本")
	flag.BoolVar(&insecure, "insecure", false, "跳过 TLS 验证")
	flag.Usage = func() { printMainHelp() }
}

func main() {
	flag.Parse()
	if showVersion {
		fmt.Printf("bbgrid-cli %s (%s) %s\n", Version, GitCommit, BuildTime)
		return
	}

	args := flag.Args()
	if len(args) == 0 {
		printMainHelp()
	}

	var err error
	cfg, err = loadConfig(configPath)
	if err != nil {
		fatal("加载配置失败: %v", err)
	}
	initHTTP()

	cmd, rest := args[0], args[1:]
	switch cmd {
	case "login":
		cmdLogin(rest)
	case "ping":
		cmdPing()
	case "status":
		cmdStatus()
	case "nodes":
		cmdNodes(rest)
	case "proxies":
		cmdProxies(rest)
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

func printMainHelp() {
	fmt.Print(`bbgrid-cli - BBgrid 网络代理管理工具

用法: bbgrid-cli [全局选项] <命令> [参数]

命令:
  login       登录服务器
  ping        健康检查
  status      服务器状态
  nodes       节点管理
  proxies     代理管理
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

func printCommandHelp(cmd string) {
	helps := map[string]string{
		"login": `用法: bbgrid-cli login -server <url> -api-key <key>

选项:
  -server <url>     服务器地址 (必填)
  -api-key <key>    API 密钥 (必填)`,

		"nodes": `用法: bbgrid-cli nodes [client-id]

  bbgrid-cli nodes              列出所有节点
  bbgrid-cli nodes <client-id>  查看节点详情`,

		"proxies": `用法: bbgrid-cli proxies [子命令] [参数]

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

	cfg.Server = strings.TrimRight(*server, "/")
	cfg.Token = result.Data.Token
	cfg.TokenExp = time.Now().Unix() + result.Data.ExpiresIn
	cfg.Insecure = cfg.Insecure || insecure

	data, _ := json.MarshalIndent(cfg, "", "  ")
	os.WriteFile(configPath, data, 0600)
	fmt.Println("登录成功")
}

func cmdPing() {
	resp, err := api("GET", "/PING", nil)
	fatalOn(err)
	print(resp)
}

func cmdStatus() {
	resp, err := api("GET", "/status", nil)
	fatalOn(err)
	print(resp)
}

func cmdNodes(args []string) {
	if len(args) > 0 {
		resp, err := api("GET", "/api/v1/nodes/"+args[0], nil)
		fatalOn(err)
		print(resp)
		return
	}

	resp, err := api("GET", "/api/v1/nodes", nil)
	fatalOn(err)

	if jsonOutput {
		printJSON(resp)
		return
	}

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
}

func cmdProxies(args []string) {
	sub := "list"
	if len(args) > 0 {
		sub = args[0]
		args = args[1:]
	}

	switch sub {
	case "list":
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
		if len(args) < 1 {
			fmt.Fprintln(os.Stderr, "用法: bbgrid-cli proxies create <client-id> -remote <port> -local <port>")
			os.Exit(1)
		}
		clientID := args[0]
		fs := flag.NewFlagSet("proxies create", flag.ExitOnError)
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
		if len(args) < 1 {
			fmt.Fprintln(os.Stderr, "用法: bbgrid-cli proxies close <port>")
			os.Exit(1)
		}
		resp, err := api("DELETE", "/api/v1/proxies/"+args[0], nil)
		fatalOn(err)
		print(resp)

	default:
		fmt.Fprintf(os.Stderr, "未知子命令: %s\n", sub)
		fmt.Fprintln(os.Stderr, "用法: bbgrid-cli proxies <list|create|close>")
		os.Exit(1)
	}
}

func cmdRelay(args []string) {
	sub := "list"
	if len(args) > 0 {
		sub = args[0]
		args = args[1:]
	}

	switch sub {
	case "list":
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

func cmdRegister(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "用法: bbgrid-cli register <apply|approve|revoke|pending|list>")
		os.Exit(1)
	}
	sub, rest := args[0], args[1:]

	switch sub {
	case "apply":
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
		resp, err := api("GET", "/api/v1/register/pending", nil)
		fatalOn(err)
		print(resp)

	case "list":
		resp, err := api("GET", "/api/v1/register/list", nil)
		fatalOn(err)
		print(resp)

	default:
		fmt.Fprintf(os.Stderr, "未知子命令: %s\n", sub)
		fmt.Fprintln(os.Stderr, "用法: bbgrid-cli register <apply|approve|revoke|pending|list>")
		os.Exit(1)
	}
}

func cmdNamespace(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "用法: bbgrid-cli namespace <list|info|clients|assign>")
		os.Exit(1)
	}
	sub, rest := args[0], args[1:]

	switch sub {
	case "list":
		resp, err := api("GET", "/api/v1/namespaces", nil)
		fatalOn(err)
		print(resp)

	case "info":
		if len(rest) < 1 {
			fmt.Fprintln(os.Stderr, "用法: bbgrid-cli namespace info <name>")
			os.Exit(1)
		}
		resp, err := api("GET", "/api/v1/namespaces/"+rest[0], nil)
		fatalOn(err)
		print(resp)

	case "clients":
		if len(rest) < 1 {
			fmt.Fprintln(os.Stderr, "用法: bbgrid-cli namespace clients <name>")
			os.Exit(1)
		}
		resp, err := api("GET", "/api/v1/namespaces/"+rest[0]+"/clients", nil)
		fatalOn(err)
		print(resp)

	case "assign":
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

func cmdSync() {
	resp, err := api("GET", "/api/v1/sync", nil)
	fatalOn(err)

	home := getHomeDir()
	syncFile := filepath.Join(home, ".aether_sync.json")
	os.WriteFile(syncFile, resp.Data, 0644)

	if jsonOutput {
		printJSON(resp)
		return
	}

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

func cmdRun(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "用法: bbgrid-cli run <action> [key=value...]")
		os.Exit(1)
	}

	action := args[0]
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

func cmdTask(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "用法: bbgrid-cli task <task-id>")
		os.Exit(1)
	}
	resp, err := api("GET", "/api/v1/tasks/"+args[0], nil)
	fatalOn(err)
	print(resp)
}

func cmdUpdate(args []string) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "用法: bbgrid-cli update <server|client>")
		os.Exit(1)
	}
	sub, rest := args[0], args[1:]

	switch sub {
	case "server":
		if len(rest) < 1 {
			fmt.Fprintln(os.Stderr, "用法: bbgrid-cli update server <binary>")
			os.Exit(1)
		}
		md5sum := calcMD5(rest[0])
		uploadBinary(rest[0], cfg.Server+"/api/v1/update", md5sum)
		fmt.Println("服务端更新成功")

	case "client":
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

func getHomeDir() string {
	usr, err := user.Current()
	if err != nil {
		return "."
	}
	return usr.HomeDir
}

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

func unmarshal(data json.RawMessage, v interface{}) {
	json.Unmarshal(data, v)
}

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

func printJSON(v interface{}) {
	data, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(data))
}

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

func printMap(m map[string]interface{}) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	for k, v := range m {
		switch val := v.(type) {
		case []interface{}:
			fmt.Fprintf(w, "%s:\n", k)
			for _, item := range val {
				if m, ok := item.(map[string]interface{}); ok {
					fmt.Fprintf(w, "  -\n")
					for k2, v2 := range m {
						fmt.Fprintf(w, "    %s:\t%v\n", k2, v2)
					}
				} else {
					fmt.Fprintf(w, "  - %v\n", item)
				}
			}
		default:
			fmt.Fprintf(w, "%s:\t%v\n", k, v)
		}
	}
	w.Flush()
}

func printList(list []interface{}) {
	for _, item := range list {
		if m, ok := item.(map[string]interface{}); ok {
			w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			for k, v := range m {
				fmt.Fprintf(w, "%s:\t%v\n", k, v)
			}
			w.Flush()
			fmt.Println("---")
		} else {
			fmt.Printf("- %v\n", item)
		}
	}
}

func calcMD5(path string) string {
	f, err := os.Open(path)
	fatalOn(err)
	defer f.Close()
	h := md5.New()
	io.Copy(h, f)
	return hex.EncodeToString(h.Sum(nil))
}

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

func fatal(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(1)
}

func fatalOn(err error) {
	if err != nil {
		fatal("%v", err)
	}
}
