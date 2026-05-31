// Package main 是 Aether CLI 工具
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
	neturl "net/url"
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
	timeout     int
	cfg         *CLIConfig
	httpClient  *http.Client
)

func init() {
	home := getHomeDir()
	flag.StringVar(&configPath, "config", filepath.Join(home, ".aether_config.json"), "配置文件路径")
	flag.BoolVar(&jsonOutput, "json", false, "JSON 输出模式")
	flag.BoolVar(&showVersion, "version", false, "版本")
	flag.BoolVar(&insecure, "insecure", false, "跳过 TLS 验证")
	flag.IntVar(&timeout, "timeout", 300, "超时时间（秒）")
	flag.Usage = func() { printMainHelp() }
}

func main() {
	flag.Parse()
	if showVersion {
		fmt.Printf("aether-cli %s (%s) %s\n", Version, GitCommit, BuildTime)
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
	fmt.Print(`aether-cli - Aether 网络代理管理工具

用法: aether-cli [全局选项] <命令> [参数]

命令:
  login       登录服务器并保存认证信息
  ping        检查服务器连接状态
  status      查看服务器运行状态
  node        节点管理 (list / view <id>)
  proxy       代理管理 (list / create / close)
  relay       中继管理 (list / create / close)
  register    注册管理 (apply / approve / revoke / pending / list)
  namespace   命名空间管理 (list / info / clients / assign)
  sync        同步插件可用操作列表
  run         执行插件操作
  task        查询异步任务状态

全局选项:
  -config <path>    配置文件路径 (默认 ~/.aether_config.json)
  -json             以 JSON 格式输出
  -insecure         跳过 TLS 证书验证
  -timeout <sec>    超时时间（秒，默认 300）
  -version          显示版本信息

帮助:
  aether-cli help <命令>    查看命令详细用法和示例
`)
	os.Exit(0)
}

func printCommandHelp(cmd string) {
	helps := map[string]string{
		"login": `用法: aether-cli login -server <url> -api-key <key>

选项:
  -server <url>     服务器地址 (必填)
  -api-key <key>    API 密钥 (必填)`,

		"node": `用法: aether-cli node <子命令> [参数]

节点管理。

子命令:
  list              列出所有在线节点
  <id>              查看指定节点详情

示例:
  aether-cli node list
  aether-cli node my-device`,

		"proxy": `用法: aether-cli proxy <子命令> [参数]

代理管理。

子命令:
  list                          列出所有活跃代理
  create <client-id> [选项]     创建端口转发代理
  close <port>                  关闭指定端口的代理

create 选项:
  -remote <port>       服务端对外端口 (必填)
  -local <port>        客户端本地端口 (必填)
  -local-ip <ip>       客户端本地 IP (默认 127.0.0.1)
  -protocol <tcp|udp>  协议 (默认 tcp)
  -bind <addr>         服务端绑定地址 (默认 0.0.0.0)

示例:
  aether-cli proxy list
  aether-cli proxy create my-device -remote 8080 -local 80
  aether-cli proxy create my-device -remote 3306 -local 3306 -local-ip 192.168.1.100
  aether-cli proxy close 8080`,

		"relay": `用法: aether-cli relay <子命令> [参数]

中继管理——建立 A端→B端 隧道。

子命令:
  list                              列出活跃中继

  create <A端> <B端> [选项]         创建中继
    A端: 对外开端口等待连接
    B端: 实际服务所在的机器
    流量: 外部 → A端:aport → relay → B端:bport

  close <会话ID>                    关闭中继

create 选项:
  -aport <port>       A端对外端口 (必填)
  -bport <port>       B端服务端口 (必填)  
  -aip <ip>           A端绑定 IP (默认 0.0.0.0)
  -bip <ip>           B端服务 IP (默认 127.0.0.1)
  -protocol <tcp|udp> 协议 (默认 tcp)

示例:
  # 在 node-a 上开 8080 端口，转发到 node-b 的 80
  aether-cli relay create node-a node-b -aport 8080 -bport 80
  # 然后在 node-a 上执行: curl localhost:8080`,

		"register": `用法: aether-cli register <子命令> [参数]

客户端注册管理，用于管理客户端接入服务器的审批流程。

子命令:
  apply     提交注册申请 (新客户端首次接入时使用)
  approve   审核通过 (管理员批准待审核的客户端)
  revoke    吊销客户端 (撤销已通过的客户端证书)
  pending   查看待审核列表 (显示等待管理员审批的客户端)
  list      查看已注册列表 (显示所有已通过审核的客户端)

apply 选项:
  -id <client-id>      客户端 ID (必填)
  -pubkey <file>       公钥文件路径 (必填)
  -token <token>       认证 token (必填)

approve 选项:
  -id <client-id>      客户端 ID (必填)
  -namespace <ns>      命名空间 (默认 permanent)
  -role <role>         角色 (默认 permanent)

revoke 选项:
  -id <client-id>      客户端 ID (必填)

示例:
  aether-cli register list                    # 查看所有已注册客户端
  aether-cli register pending                  # 查看待审核客户端
  aether-cli register approve -id my-device    # 审核通过指定客户端
  aether-cli register apply -id new-device -pubkey client.pub -token xxx
  aether-cli register revoke -id old-device    # 吊销客户端`,

		"namespace": `用法: aether-cli namespace <子命令> [参数]

命名空间管理，用于组织和管理客户端分组。

子命令:
  list                              列出所有命名空间
  info <name>                       查看命名空间详情
  clients <name>                    列出命名空间下的客户端
  assign <client-id> <ns> <role>    将客户端分配到命名空间

示例:
  aether-cli namespace list
  aether-cli namespace info permanent
  aether-cli namespace clients permanent
  aether-cli namespace assign my-device production worker`,

		"sync": `用法: aether-cli sync

同步服务器可用的操作列表（插件提供的操作），结果缓存到 ~/.aether_sync.json

示例:
  aether-cli sync`,

		"run": `用法: aether-cli run <action> [key=value...]

执行插件提供的操作（异步执行，返回任务 ID）。
参数以 key=value 形式传入。

示例:
  aether-cli run latency.get client_id=node-01
  aether-cli run tag.set client_id=node-01 key=env value=prod

提示: 使用 'aether-cli sync' 先同步可用操作列表`,
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
		fmt.Fprintln(os.Stderr, "用法: aether-cli login -server <url> -api-key <key>")
		os.Exit(1)
	}

	*server = normalizeServerURL(*server)

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

	saveConfig(configPath, cfg)
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

	if jsonOutput {
		printJSON(resp)
		return
	}

	var data struct {
		StartTime string `json:"start_time"`
		Uptime    string `json:"uptime"`
		Version   string `json:"version"`
		PublicIP  string `json:"public_ip"`
		Stats     struct {
			Clients int `json:"clients"`
			Proxies int `json:"proxies"`
			Relays  int `json:"relays"`
		} `json:"stats"`
		Components map[string]struct {
			Status string `json:"status"`
			Uptime string `json:"uptime"`
		} `json:"components"`
		Plugins []struct {
			Name    string `json:"name"`
			Version string `json:"version"`
			Actions []struct {
				Name        string `json:"name"`
				Description string `json:"description"`
			} `json:"actions"`
		} `json:"plugins_static"`
	}
	unmarshal(resp.Data, &data)

	fmt.Printf("服务器:     %s\n", data.PublicIP)
	fmt.Printf("版本:       %s\n", data.Version)
	fmt.Printf("运行时间:   %s\n", data.Uptime)
	fmt.Printf("节点数:     %d\n", data.Stats.Clients)
	fmt.Printf("代理数:     %d\n", data.Stats.Proxies)
	fmt.Printf("中继数:     %d\n", data.Stats.Relays)

	if len(data.Components) > 0 {
		fmt.Println("\n组件:")
		for name, comp := range data.Components {
			fmt.Printf("  %-12s %s\n", name, comp.Status)
		}
	}
}

func cmdNode(args []string) {
	if len(args) == 0 {
		printCommandHelp("node")
		return
	}

	sub := args[0]
	args = args[1:]

	switch sub {
	case "list":
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
		fmt.Fprintln(w, "ID\t地址\t状态\t代理数")
		fmt.Fprintln(w, "--\t----\t----\t------")
		for _, c := range data.Clients {
			status := "离线"
			if c.Online {
				status = "在线"
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%d\n", c.ID, c.RemoteAddr, status, c.ProxyCount)
		}
		w.Flush()

	default:
		// 当作 node id 查询
		resp, err := api("GET", "/api/v1/nodes/"+sub, nil)
		fatalOn(err)
		if jsonOutput {
			printJSON(resp)
		} else {
			printNodeDetail(resp)
		}
	}
}

func printNodeDetail(resp *Response) {
	var data struct {
		ID         string `json:"id"`
		RemoteAddr string `json:"remote_addr"`
		ProxyCount int    `json:"proxy_count"`
		Host       string `json:"host"`
		Online     bool   `json:"online"`
	}
	unmarshal(resp.Data, &data)

	status := "离线"
	if data.Online {
		status = "在线"
	}
	fmt.Printf("ID:       %s\n", data.ID)
	fmt.Printf("地址:     %s\n", data.RemoteAddr)
	fmt.Printf("状态:     %s\n", status)
	fmt.Printf("代理数:   %d\n", data.ProxyCount)
}

func cmdProxy(args []string) {
	if len(args) == 0 {
		printCommandHelp("proxy")
		return
	}

	sub := args[0]
	args = args[1:]

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
				Protocol   string `json:"protocol"`
				BindAddr   string `json:"bind_addr"`
			} `json:"proxies"`
		}
		unmarshal(resp.Data, &data)

		if len(data.Proxies) == 0 {
			fmt.Println("没有代理")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "客户端\t远程端口\t本地端口\t协议\t绑定地址")
		fmt.Fprintln(w, "------\t--------\t--------\t----\t--------")
		for _, p := range data.Proxies {
			fmt.Fprintf(w, "%s\t%d\t%d\t%s\t%s\n", p.ClientID, p.RemotePort, p.LocalPort, p.Protocol, p.BindAddr)
		}
		w.Flush()

	case "create":
		clientID := ""
		flagArgs := args
		if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
			clientID = args[0]
			flagArgs = args[1:]
		}

		fs := flag.NewFlagSet("proxy create", flag.ExitOnError)
		remote := fs.Int("remote", 0, "服务端端口")
		local := fs.Int("local", 0, "客户端端口")
		localIP := fs.String("local-ip", "127.0.0.1", "客户端 IP")
		protocol := fs.String("protocol", "tcp", "协议")
		bind := fs.String("bind", "0.0.0.0", "绑定地址")
		fs.Parse(flagArgs)

		if *remote == 0 || *local == 0 {
			fmt.Fprintln(os.Stderr, "错误: -remote 和 -local 必填")
			os.Exit(1)
		}

		if clientID == "" && fs.NArg() > 0 {
			clientID = fs.Arg(0)
		}

		resp, err := api("POST", "/api/v1/proxies", map[string]any{
			"client_id":   clientID,
			"remote_port": *remote,
			"local_port":  *local,
			"local_ip":    *localIP,
			"protocol":    *protocol,
			"bind_addr":   *bind,
		})
		fatalOn(err)
		print(resp)

	case "close":
		if len(args) == 0 {
			fmt.Fprintln(os.Stderr, "错误: 需要端口号")
			os.Exit(1)
		}
		port := args[0]
		resp, err := api("DELETE", "/api/v1/proxies/"+port, nil)
		fatalOn(err)
		print(resp)

	default:
		fmt.Fprintf(os.Stderr, "未知子命令: %s\n", sub)
		fmt.Fprintln(os.Stderr, "用法: aether-cli proxy <list|create|close>")
		os.Exit(1)
	}
}

func cmdRelay(args []string) {
	if len(args) == 0 {
		printCommandHelp("relay")
		return
	}

	sub := args[0]
	args = args[1:]

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
		fmt.Fprintln(w, "会话ID\tA端(入口)\tB端(服务)\t协议")
		fmt.Fprintln(w, "-------\t-------\t--------\t----")
		for _, s := range data.Sessions {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", s.SessionID, s.SourceClient, s.TargetClient, s.Protocol)
		}
		w.Flush()

	case "create":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "用法: aether-cli relay create <A端> <B端> -aport <port> -bport <port>")
			os.Exit(1)
		}
		aSide, bSide := args[0], args[1]
		fs := flag.NewFlagSet("relay create", flag.ExitOnError)
		aPort := fs.Int("aport", 0, "A端对外端口")
		bPort := fs.Int("bport", 0, "B端服务端口")
		protocol := fs.String("protocol", "tcp", "协议")
		bIP := fs.String("bip", "127.0.0.1", "B端服务 IP")
		aIP := fs.String("aip", "0.0.0.0", "A端绑定 IP")
		fs.Parse(args[2:])
		if *aPort == 0 || *bPort == 0 {
			fmt.Fprintln(os.Stderr, "错误: -aport 和 -bport 必填")
			os.Exit(1)
		}
		resp, err := api("POST", "/api/v1/relay", map[string]any{
			"source_client_id": aSide, "target_client_id": bSide,
			"source_port": *aPort, "target_port": *bPort,
			"protocol": *protocol, "target_local_ip": *bIP, "source_local_ip": *aIP,
		})
		fatalOn(err)
		print(resp)

	case "close":
		if len(args) < 1 {
			fmt.Fprintln(os.Stderr, "用法: aether-cli relay close <session-id>")
			os.Exit(1)
		}
		resp, err := api("DELETE", "/api/v1/relay/"+args[0], nil)
		fatalOn(err)
		print(resp)

	default:
		fmt.Fprintf(os.Stderr, "未知子命令: %s\n", sub)
		fmt.Fprintln(os.Stderr, "用法: aether-cli relay <list|create|close>")
		os.Exit(1)
	}
}

func cmdRegister(args []string) {
	if len(args) == 0 {
		printCommandHelp("register")
		os.Exit(0)
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
		if jsonOutput {
			printJSON(resp)
		} else {
			printRegisterList(resp)
		}

	case "list":
		resp, err := api("GET", "/api/v1/register/list", nil)
		fatalOn(err)
		if jsonOutput {
			printJSON(resp)
		} else {
			printRegisterList(resp)
		}

	default:
		fmt.Fprintf(os.Stderr, "未知子命令: %s\n", sub)
		fmt.Fprintln(os.Stderr, "用法: aether-cli register <apply|approve|revoke|pending|list>")
		os.Exit(1)
	}
}

func cmdNamespace(args []string) {
	if len(args) == 0 {
		printCommandHelp("namespace")
		os.Exit(0)
	}
	sub, rest := args[0], args[1:]

	switch sub {
	case "list":
		resp, err := api("GET", "/api/v1/namespaces", nil)
		fatalOn(err)
		if jsonOutput {
			printJSON(resp)
			return
		}
		printNamespaceList(resp)

	case "info":
		if len(rest) < 1 {
			fmt.Fprintln(os.Stderr, "用法: aether-cli namespace info <name>")
			os.Exit(1)
		}
		resp, err := api("GET", "/api/v1/namespaces/"+rest[0], nil)
		fatalOn(err)
		print(resp)

	case "clients":
		if len(rest) < 1 {
			fmt.Fprintln(os.Stderr, "用法: aether-cli namespace clients <name>")
			os.Exit(1)
		}
		resp, err := api("GET", "/api/v1/namespaces/"+rest[0]+"/clients", nil)
		fatalOn(err)
		print(resp)

	case "assign":
		if len(rest) < 3 {
			fmt.Fprintln(os.Stderr, "用法: aether-cli namespace assign <client-id> <namespace> <role>")
			os.Exit(1)
		}
		resp, err := api("POST", "/api/v1/namespaces/assign", map[string]string{
			"client_id": rest[0], "namespace": rest[1], "role": rest[2],
		})
		fatalOn(err)
		print(resp)

	default:
		fmt.Fprintf(os.Stderr, "未知子命令: %s\n", sub)
		fmt.Fprintln(os.Stderr, "用法: aether-cli namespace <list|info|clients|assign>")
		os.Exit(1)
	}
}

func cmdSync() {
	resp, err := api("GET", "/api/v1/runtime/capabilities", nil)
	fatalOn(err)

	home := getHomeDir()
	syncFile := filepath.Join(home, ".aether_sync.json")
	os.WriteFile(syncFile, resp.Data, 0644)

	if jsonOutput {
		printJSON(resp)
		return
	}

	var caps []struct {
		Name        string `json:"name"`
		Description string `json:"description,omitempty"`
	}
	unmarshal(resp.Data, &caps)

	if len(caps) == 0 {
		fmt.Println("没有可用操作")
		return
	}

	fmt.Println("可用操作:")
	for _, c := range caps {
		desc := c.Description
		if desc == "" {
			desc = "-"
		}
		fmt.Printf("  %-20s %s\n", c.Name, desc)
	}
}

func cmdRun(args []string) {
	if len(args) == 0 {
		printCommandHelp("run")
		os.Exit(0)
	}

	action := args[0]
	params := make(map[string]any)
	var filePath string

	for _, arg := range args[1:] {
		if !strings.Contains(arg, "=") && filePath == "" {
			filePath = arg
			continue
		}
		parts := strings.SplitN(arg, "=", 2)
		if len(parts) == 2 {
			key := strings.TrimLeft(parts[0], "-")
			params[key] = parts[1]
		}
	}

	// 文件上传走 multipart
	if filePath != "" {
		resp, err := apiUpload(action, filePath, params)
		if err != nil {
			fatal("请求失败: %v", err)
		}
		print(resp)
		return
	}

	// 文件下载通过 /runtime/call 获取下载 URL，再请求下载
	if action == "file.pull" {
		filename, _ := params["filename"].(string)
		clientID, _ := params["client_id"].(string)
		if filename == "" || clientID == "" {
			fatal("用法: aether-cli run file.pull client_id=<id> filename=<文件>")
		}
		err := apiDownloadStream("file.pull", map[string]any{"path": filename}, filepath.Base(filename))
		if err != nil {
			fatal("下载失败: %v", err)
		}
		return
	}

	// 使用新接口 /runtime/call
	resp, err := api("POST", "/api/v1/runtime/call", map[string]any{
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

	print(resp)
}

func cmdTask(args []string) {
	if len(args) == 0 {
		printCommandHelp("task")
		os.Exit(0)
	}
	resp, err := api("GET", "/api/v1/tasks/"+args[0], nil)
	fatalOn(err)
	print(resp)
}

// ==================== 辅助函数 ====================

func showActions(input string) {
	home := getHomeDir()
	data, err := os.ReadFile(filepath.Join(home, ".aether_sync.json"))
	if err != nil {
		return
	}
	var syncData struct {
		Capabilities []struct {
			Name        string `json:"name"`
			Description string `json:"description,omitempty"`
			Transport   string `json:"transport"`
		} `json:"data"`
	}
	if json.Unmarshal(data, &syncData) != nil || len(syncData.Capabilities) == 0 {
		return
	}
	fmt.Fprintln(os.Stderr, "\n可用操作:")
	for _, c := range syncData.Capabilities {
		desc := c.Description
		if desc == "" {
			desc = "-"
		}
		fmt.Fprintf(os.Stderr, "  %-20s %-10s %s\n", c.Name, c.Transport, desc)
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
		Timeout: time.Duration(timeout) * time.Second,
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
	} else if cfg.APIKey != "" {
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

	// Token 过期或无效时，自动用 API key 重试
	if resp.StatusCode == http.StatusUnauthorized && cfg.Token != "" && cfg.APIKey != "" {
		cfg.Token = ""
		saveConfig(configPath, cfg)

		var retryReader io.Reader
		if body != nil {
			data, _ := json.Marshal(body)
			retryReader = bytes.NewReader(data)
		}
		retryReq, err := http.NewRequest(method, cfg.Server+path, retryReader)
		if err != nil {
			return nil, err
		}
		retryReq.Header.Set("X-API-KEY", cfg.APIKey)
		if body != nil {
			retryReq.Header.Set("Content-Type", "application/json")
		}

		resp2, err := httpClient.Do(retryReq)
		if err != nil {
			return nil, err
		}
		defer resp2.Body.Close()

		respBody, err = io.ReadAll(resp2.Body)
		if err != nil {
			return nil, err
		}
		resp = resp2
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	var result Response
	json.Unmarshal(respBody, &result)
	return &result, nil
}

// apiUpload 上传文件（multipart/form-data）
func apiUpload(action, filePath string, params map[string]any) (*Response, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("打开文件失败: %w", err)
	}
	defer f.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	// 添加文件
	part, err := writer.CreateFormFile("file", filepath.Base(filePath))
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(part, f); err != nil {
		return nil, err
	}
	writer.Close()

	u, err := neturl.Parse(cfg.Server + "/api/v1/runtime/call")
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("action", action)
	for k, v := range params {
		q.Set(k, fmt.Sprintf("%v", v))
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequest("POST", u.String(), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	if cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.Token)
	} else {
		req.Header.Set("X-API-KEY", cfg.APIKey)
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

// apiDownloadURL 使用服务端返回的 URL 下载文件
func apiDownloadStream(action string, params map[string]any, saveName string) error {
	body, _ := json.Marshal(map[string]any{"action": action, "params": params})
	req, err := http.NewRequest("POST", cfg.Server+"/api/v1/runtime/call", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if cfg.Token != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.Token)
	} else {
		req.Header.Set("X-API-KEY", cfg.APIKey)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	out, err := os.Create(saveName)
	if err != nil {
		return fmt.Errorf("创建文件失败: %w", err)
	}
	defer out.Close()

	written, err := io.Copy(out, resp.Body)
	if err != nil {
		return fmt.Errorf("保存失败: %w", err)
	}

	fmt.Printf("下载成功: %s (%d bytes)\n", saveName, written)
	return nil
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
	cfg.Server = normalizeServerURL(cfg.Server)
	return &cfg, nil
}

// normalizeServerURL 给没有 scheme 的地址补上 https://
func normalizeServerURL(u string) string {
	u = strings.TrimRight(u, "/")
	if u == "" {
		return u
	}
	if strings.HasPrefix(u, "http://") || strings.HasPrefix(u, "https://") {
		return u
	}
	return "https://" + u
}

func saveConfig(path string, cfg *CLIConfig) {
	data, _ := json.MarshalIndent(cfg, "", "  ")
	os.WriteFile(path, data, 0600)
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
		// ActionResult 包装：提取内层 code/msg/data
		if m, ok := data.(map[string]interface{}); ok {
			msg, _ := m["msg"].(string)
			inner, hasData := m["data"]

			if msg != "" {
				fmt.Printf("msg:\t%s\n", msg)
			}
			if !hasData || inner == nil {
				return
			}
			printData(inner)
			return
		}
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

func printList(l []interface{}) {
	if len(l) == 0 {
		fmt.Println("(空)")
		return
	}

	// 检查是否是 map 列表
	if _, ok := l[0].(map[string]interface{}); ok {
		// 检查是否有 client_id 字段，按 client_id 分组
		if hasField(l, "client_id") {
			printGroupedByClient(l)
			return
		}
		printTable(l)
		return
	}

	for i, v := range l {
		fmt.Printf("[%d] %v\n", i, v)
	}
}

// hasField 检查列表中的 map 是否有指定字段
func hasField(l []interface{}, field string) bool {
	for _, item := range l {
		if m, ok := item.(map[string]interface{}); ok {
			if _, exists := m[field]; exists {
				return true
			}
		}
	}
	return false
}

// printGroupedByClient 按 client_id 分组展示
func printGroupedByClient(l []interface{}) {
	// 按 client_id 分组
	groups := make(map[string][]map[string]interface{})
	for _, item := range l {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		clientID, _ := m["client_id"].(string)
		if clientID == "" {
			clientID = "(未知)"
		}
		groups[clientID] = append(groups[clientID], m)
	}

	for clientID, items := range groups {
		fmt.Printf("%s:\n", clientID)
		// 去掉 client_id 列，只显示其他字段
		cleaned := make([]interface{}, 0, len(items))
		for _, m := range items {
			cp := make(map[string]interface{}, len(m)-1)
			for k, v := range m {
				if k != "client_id" {
					cp[k] = v
				}
			}
			cleaned = append(cleaned, cp)
		}
		if len(cleaned) == 0 {
			fmt.Println("  (无)")
			continue
		}
		// 如果只剩一个 map 且都是简单字段，用 key: value 格式
		if len(cleaned) == 1 {
			if m, ok := cleaned[0].(map[string]interface{}); ok {
				printMapIndented(m, "  ")
				continue
			}
		}
		// 否则用表格
		printTableIndented(cleaned, "  ")
	}
	fmt.Println()
}

func printMapIndented(m map[string]interface{}, indent string) {
	for k, v := range m {
		switch val := v.(type) {
		case map[string]interface{}:
			fmt.Printf("%s%s:\n", indent, k)
			printMapIndented(val, indent+"  ")
		case []interface{}:
			if len(val) == 0 {
				fmt.Printf("%s%s: (空)\n", indent, k)
			} else {
				fmt.Printf("%s%s:\n", indent, k)
				for _, item := range val {
					fmt.Printf("%s  %v\n", indent, item)
				}
			}
		default:
			fmt.Printf("%s%s: %v\n", indent, k, formatCell(k, v))
		}
	}
}

func printTableIndented(l []interface{}, indent string) {
	if len(l) == 0 {
		return
	}
	first := l[0].(map[string]interface{})
	keys := make([]string, 0, len(first))
	for k := range first {
		keys = append(keys, k)
	}

	widths := make(map[string]int)
	for _, k := range keys {
		widths[k] = len(k)
	}
	for _, item := range l {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		for _, k := range keys {
			cell := formatCell(k, m[k])
			w := len(cell)
			if w > widths[k] {
				widths[k] = w
			}
		}
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	for _, k := range keys {
		fmt.Fprintf(w, "%s%-*s\t", indent, widths[k], k)
	}
	fmt.Fprintln(w)
	for _, k := range keys {
		fmt.Fprintf(w, "%s%s\t", indent, strings.Repeat("-", widths[k]))
	}
	fmt.Fprintln(w)

	for _, item := range l {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		for _, k := range keys {
			fmt.Fprintf(w, "%s%-*s\t", indent, widths[k], formatCell(k, m[k]))
		}
		fmt.Fprintln(w)
	}
	w.Flush()
}

func printMap(m map[string]interface{}) {
	simpleKeys := []string{}
	for k, v := range m {
		switch v.(type) {
		case string, float64, bool, int, int64:
			simpleKeys = append(simpleKeys, k)
		}
	}
	for _, k := range simpleKeys {
		switch val := m[k].(type) {
		case string:
			display := val
			if len(display) > 100 {
				display = display[:50] + "..." + display[len(display)-20:]
			}
			fmt.Printf("%s:\t%s\n", k, display)
		case float64:
			if strings.Contains(strings.ToLower(k), "size") {
				fmt.Printf("%s:\t%s\n", k, humanSize(int64(val)))
			} else {
				fmt.Printf("%s:\t%v\n", k, val)
			}
		default:
			fmt.Printf("%s:\t%v\n", k, m[k])
		}
		delete(m, k)
	}
	for k, v := range m {
		if arr, ok := v.([]interface{}); ok {
			fmt.Printf("%s:\n", k)
			printList(arr)
		} else if subMap, ok := v.(map[string]interface{}); ok {
			fmt.Printf("%s:\n", k)
			printMap(subMap)
		} else {
			fmt.Printf("%s:\t%v\n", k, v)
		}
	}
}

func printTable(l []interface{}) {
	if len(l) == 0 {
		return
	}

	// 收集所有列名
	first := l[0].(map[string]interface{})
	keys := make([]string, 0, len(first))
	for k := range first {
		keys = append(keys, k)
	}

	// 计算每列最大宽度
	widths := make(map[string]int)
	for _, k := range keys {
		widths[k] = len(k)
	}
	for _, item := range l {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		for _, k := range keys {
			cell := formatCell(k, m[k])
			w := len(cell)
			if w > widths[k] {
				widths[k] = w
			}
		}
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	for _, k := range keys {
		fmt.Fprintf(w, "%-*s\t", widths[k], k)
	}
	fmt.Fprintln(w)
	for _, k := range keys {
		fmt.Fprintf(w, "%s\t", strings.Repeat("-", widths[k]))
	}
	fmt.Fprintln(w)

	for _, item := range l {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		for _, k := range keys {
			fmt.Fprintf(w, "%-*s\t", widths[k], formatCell(k, m[k]))
		}
		fmt.Fprintln(w)
	}
	w.Flush()
}

func printNamespaceList(resp *Response) {
	var data struct {
		Namespaces []struct {
			Name        string  `json:"name"`
			Description string  `json:"description"`
			Type        string  `json:"type"`
			Clients     []any   `json:"clients"`
			CreatedAt   float64 `json:"created_at"`
		} `json:"namespaces"`
	}
	unmarshal(resp.Data, &data)

	if len(data.Namespaces) == 0 {
		fmt.Println("没有命名空间")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "名称\t类型\t客户端数\t描述\t创建时间")
	fmt.Fprintln(w, "----\t----\t--------\t----\t--------")
	for _, ns := range data.Namespaces {
		created := ""
		if ns.CreatedAt > 0 {
			created = time.Unix(int64(ns.CreatedAt), 0).Format("2006-01-02 15:04")
		}
		fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%s\n", ns.Name, ns.Type, len(ns.Clients), ns.Description, created)
	}
	w.Flush()
}

func printRegisterList(resp *Response) {
	var data struct {
		Clients []struct {
			ClientID    string `json:"client_id"`
			PublicKey   string `json:"public_key"`
			Certificate string `json:"certificate"`
			Status      string `json:"status"`
			Namespace   string `json:"namespace"`
			Role        string `json:"role"`
			CreatedAt   int64  `json:"created_at"`
			ApprovedAt  int64  `json:"approved_at"`
		} `json:"clients"`
	}
	unmarshal(resp.Data, &data)

	clients := data.Clients
	if clients == nil {
		// pending 接口返回 applications 字段
		var pending struct {
			Applications []struct {
				ClientID    string `json:"client_id"`
				PublicKey   string `json:"public_key"`
				Certificate string `json:"certificate"`
				Status      string `json:"status"`
				Namespace   string `json:"namespace"`
				Role        string `json:"role"`
				CreatedAt   int64  `json:"created_at"`
				ApprovedAt  int64  `json:"approved_at"`
			} `json:"applications"`
		}
		unmarshal(resp.Data, &pending)
		clients = pending.Applications
	}

	if len(clients) == 0 {
		fmt.Println("没有客户端")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\t状态\t命名空间\t角色\t创建时间")
	fmt.Fprintln(w, "--\t----\t--------\t----\t--------")
	for _, c := range clients {
		created := ""
		if c.CreatedAt > 0 {
			created = time.Unix(int64(c.CreatedAt), 0).Format("2006-01-02 15:04")
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", c.ClientID, c.Status, c.Namespace, c.Role, created)
	}
	w.Flush()
}

func formatCell(key string, val interface{}) string {
	if strings.Contains(strings.ToLower(key), "size") {
		switch v := val.(type) {
		case float64:
			return humanSize(int64(v))
		case int:
			return humanSize(int64(v))
		case int64:
			return humanSize(v)
		}
	}
	switch v := val.(type) {
	case map[string]interface{}:
		// map → key=value, key2=value2
		parts := make([]string, 0, len(v))
		for k, val := range v {
			parts = append(parts, fmt.Sprintf("%s=%v", k, val))
		}
		return strings.Join(parts, ", ")
	case []interface{}:
		// array → 元素用逗号分隔
		parts := make([]string, 0, len(v))
		for _, item := range v {
			parts = append(parts, fmt.Sprintf("%v", item))
		}
		return strings.Join(parts, ", ")
	default:
		return fmt.Sprintf("%v", val)
	}
}

func humanSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
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
