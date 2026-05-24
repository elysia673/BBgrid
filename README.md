# BBgrid

一个模块化的网络代理平台，从核心内核出发，通过插件拼装出你需要的架构

## 特性

- **插件驱动**: 核心内核 + 插件积木，按需组装你的架构
- **通用事件系统**: 基于 ResourceKey 的事件分发，Core 不理解业务逻辑
- **能力协商**: 插件启动时自动注册能力，即插即用
- **持久化层**: 事件存储 + 快照存储 + 元数据存储，支持崩溃恢复
- **最终一致**: Reconcile Loop 自动同步状态，即使丢消息也能收敛
- **多协议支持**: TCP、UDP、WebSocket 隧道模式
- **TCP 隧道**: 参考 autossh 设计，每连接独立隧道，可靠性高
- **UDP 隧道**: 基于 KCP 协议的可靠 UDP 隧道，低延迟、高吞吐
- **WebSocket 多路复用**: 浏览器友好的 WS 隧道，单连接多通道
- **Namespace 隔离**: 支持 temporary/permanent/mediated 三种命名空间
- **安全认证**: mTLS + JWT Token + API Key 三重认证
- **Docker 一键部署**: 自签名/本地证书两种部署脚本

## 快速开始

### 1. 编译

```bash
# 编译当前平台 (server + client + cli + runtime)
make build

# 只编译某个组件
make server
make client
make cli
make runtime

# 代码检查 (格式化 + vet + 测试)
make check
```

### 2. 启动 Server

```bash
# 复制示例配置
mkdir -p data
cp examples/server.json.example data/config.json

# 编辑配置
vim data/config.json
```

### 3. 启动 Runtime（可选）

Runtime 负责 WireGuard 接口管理，独立进程运行：

```bash
# 连接 Server 的 gRPC 端口
./bin/bbgrid-runtime -server localhost:9910 -id runtime-1

# 干运行模式（只打印命令不执行，调试用）
./bin/bbgrid-runtime -server localhost:9910 -id runtime-1 -dry-run
```

### 4. CLI 登录

```bash
bbgrid-cli login -server https://your-server:9909 -api-key "your-api-key"
bbgrid-cli sync
bbgrid-cli status
```

### 5. 部署客户端

客户端支持 JSON 配置文件或环境变量两种方式：

**方式一：JSON 配置**

```bash
cp examples/client.json.example client.json
# 编辑 client.json，填入 server_url 和 client_token
./bin/bbgrid-client -config client.json
```

**方式二：环境变量**

```bash
cp examples/env.example .env
# 编辑 .env，填入 AETHER_WS_URL 和 AETHER_CLIENT_TOKEN
./bin/bbgrid-client
```

### 6. 创建代理

```bash
# 注册客户端
bbgrid-cli register apply -id my-device -pubkey my.pub -token "your-client-token"
bbgrid-cli register approve -id my-device -namespace permanent -role permanent

# 创建代理映射
bbgrid-cli proxy create -id my-device -remote 8080 -local 8080 -protocol tcp
```

## 架构

### 程序结构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           BBgrid Server (核心)                              │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Supervisor                                                        │   │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐     │   │
│  │  │  Auth   │ │  State  │ │  Data   │ │ Control │ │   WS    │     │   │
│  │  │ Worker  │ │ Worker  │ │ Worker  │ │ Worker  │ │ Worker  │     │   │
│  │  └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│       │                                                                     │
│       ▼                                                                     │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────┐                   │
│  │  Dispatcher  │  │ StateStore   │  │ ActionHandler  │                   │
│  │ (事件分发器)  │  │ (状态接口)    │  │ (路由注册)     │                   │
│  └──────┬───────┘  └──────────────┘  └────────────────┘                   │
│         │ 按 ResourceKey 分发                                               │
│         ▼                                                                   │
│  ┌─────────────────────────────────────────────────────┐                   │
│  │              StorageManager (持久化层)               │                   │
│  │  ┌─────────────┐ ┌─────────────┐ ┌──────────────┐ │                   │
│  │  │ EventStore  │ │ SnapshotStore│ │  MetaStore   │ │                   │
│  │  │ (事件流)     │ │ (快照)       │ │ (元数据)     │ │                   │
│  │  └─────────────┘ └─────────────┘ └──────────────┘ │                   │
│  └─────────────────────────────────────────────────────┘                   │
│                                                                             │
│  HTTP :9909    gRPC :9910 (127.0.0.1)    Tunnel :9908                      │
└─────────────────────────────────────────────────────────────────────────────┘
              │                           │
              │ init() 自动注册            │ gRPC Topology streaming
              │ + 能力协商                 │
              ▼                           ▼
┌─────────────────────────────┐  ┌─────────────────────────────────┐
│       官方插件 (内置)        │  │      外部 Runtime (gRPC)        │
├─────────────────────────────┤  ├─────────────────────────────────┤
│                             │  │                                 │
│  latency   延迟监控          │  │  BBgrid Runtime                │
│  ├─ latency.get             │  │  ├─ Subscribe(topology)         │
│  └─ latency.list            │  │  ├─ ListTopologies (reconcile)  │
│                             │  │  ├─ 创建 wg 接口               │
│  persist   状态持久化        │  │  ├─ 管理 peer / route          │
│  ├─ 定时 Export/Import      │  │  └─ diff + patch (不重建)      │
│  └─ relay / proxy provider  │  │                                 │
│                             │  │  独立进程，可部署在任意机器     │
│  tag       标签管理          │  │  需要 root / netlink 权限       │
│  ├─ tag.set / tag.get       │  │                                 │
│  ├─ tag.delete / tag.list   │  └─────────────────────────────────┘
│  └─ 按标签筛选客户端         │
│                             │
└─────────────────────────────┘
```

- **核心**：5 个 Worker + Dispatcher + StorageManager，只做状态管理和消息路由
- **Dispatcher**：基于 ResourceKey 的事件分发器，Core 不理解业务逻辑
- **StorageManager**：事件存储 + 快照存储 + 元数据存储，支持崩溃恢复
- **官方插件**：跟随 Server 编译，`init()` 自动注册，通过能力协商声明能处理的资源
- **外部 Runtime**：独立进程，通过 gRPC 订阅 topology，自带 Reconcile Loop

### 核心架构

```
┌─────────────────────────────────────────────────────────────┐
│                         Main                                │
│  ┌─────────────────────────────────────────────────────┐   │
│  │                    Supervisor                        │   │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐   │   │
│  │  │  Auth   │ │  State  │ │  Data   │ │ Control │   │   │
│  │  │ Worker  │ │ Worker  │ │ Worker  │ │ Worker  │   │   │
│  │  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘   │   │
│  │       └───────────┴───────────┴───────────┘         │   │
│  │                      │                               │   │
│  │              ┌───────┴───────┐                       │   │
│  │              ▼               ▼                       │   │
│  │        ┌──────────┐   ┌─────────────┐               │   │
│  │        │   Bus    │   │ Dispatcher  │               │   │
│  │        │ (旧兼容)  │   │ (事件分发)  │               │   │
│  │        └──────────┘   └─────────────┘               │   │
│  │  ┌─────────┐                                         │   │
│  │  │   WS    │                                         │   │
│  │  │ Worker  │                                         │   │
│  │  └─────────┘                                         │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌─────────────────┐  ┌─────────────────┐  ┌────────────┐ │
│  │ HTTP Server     │  │ Tunnel Listener │  │ gRPC Server│ │
│  │ (Gin)           │  │ (TCP/UDP)       │  │ (Topology) │ │
│  └─────────────────┘  └─────────────────┘  └────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### 控制面/数据面分离

```
┌─────────────────────────────────────────────────────────────┐
│ Control Plane (管理态)                                       │
├─────────────────────────────────────────────────────────────┤
│ Auth Worker: CA、JWT、注册表、命名空间                         │
│ State Worker: 客户端状态、代理状态、中继会话 + 持久化          │
│ Control Worker: REST API、命令分发                            │
│ WS Worker: WebSocket 连接管理                                │
├─────────────────────────────────────────────────────────────┤
│ Dispatcher: 按 ResourceKey 分发 GenericEvent (不理解业务)    │
│ StorageManager: EventStore + SnapshotStore + MetaStore       │
├─────────────────────────────────────────────────────────────┤
│ 特点: 低频、带锁、可阻塞、毫秒级                              │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│ Data Plane (转发态)                                          │
├─────────────────────────────────────────────────────────────┤
│ Data Worker: TCP/UDP 代理、隧道配对、字节流 Pipe              │
├─────────────────────────────────────────────────────────────┤
│ 特点: 高并发、零锁、零分配、微秒级                            │
└─────────────────────────────────────────────────────────────┘
```

### 架构约束

每个模块必须遵守以下三条护栏原则：

**1. State Ownership — 每个状态有唯一写入者**

| 状态 | 唯一写入者 |
|------|-----------|
| `clients` / `portIndex` | StateWorker |
| `relaySessions` / `desiredRelays` | StateWorker |
| `desiredProxies` | StateWorker |
| `proxies` (per client) | ControlWorker → StateWorker |
| `multiplexers` (per client) | WSWorker → ClientState 接口 |
| `tunnelTokens` (per client) | ControlWorker → StateWorker |
| Auth 注册表 / 命名空间 | AuthWorker |
| Data 面 pending/listeners | DataWorker |
| 事件/快照/元数据 | StorageManager |

**2. Event 语义 — event 只能是事实，不能是指令**

- GenericEvent 使用 ResourceKey 标识资源：`type/namespace/name`
- 事件类型：`ADDED`、`MODIFIED`、`DELETED`
- 事件描述"发生了什么"，不描述"要去做什么"
- Dispatcher 按资源类型路由，不理解业务逻辑

**3. Registry 角色 — 只做发现，不做决策**

- Plugin registry：name → factory 的纯映射表
- Capability registry：resourceType → handler 的纯映射表
- Persist provider registry：name → provider 的纯映射表
- Action handler：action name → HTTP handler 的纯路由表

### 插件系统

```
┌─────────────────────────────────────────────────────────────┐
│ Plugin (声明能力 + 声明资源能力)                              │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  // Actions: 声明"能做什么"                                   │
│  func (p *LatencyPlugin) Actions() []plugin.Action {        │
│      return []plugin.Action{                                │
│          {Name: "latency.get", Description: "获取延迟"},     │
│      }                                                      │
│  }                                                          │
│                                                             │
│  // Capabilities: 声明"能处理什么资源"                        │
│  func (p *MyPlugin) Capabilities() []plugin.Capability {    │
│      return []plugin.Capability{                            │
│          {ResourceType: "proxy", EventTypes: [...]},        │
│      }                                                      │
│  }                                                          │
│                                                             │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│ Server (自动路由)                                            │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  // initPlugins 自动注册能力                                  │
│  for _, cap := range plugin.Capabilities() {                │
│      dispatcher.SubscribeByType(cap.ResourceType, handler)  │
│  }                                                          │
│                                                             │
│  // StateWorker 状态变更时自动触发                           │
│  state.AddProxy(...)                                        │
│    → emitEvent(GenericEvent{...})                           │
│    → Dispatcher.Dispatch(event)                             │
│    → StorageManager 持久化                                   │
│                                                             │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│ 持久化层                                                     │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  EventStore:    追加写入，不可变日志 (JSONL)                  │
│  SnapshotStore: 定期快照，快速恢复 (JSON)                    │
│  MetaStore:     资源 CRUD，按类型存储 (JSON)                 │
│                                                             │
│  崩溃恢复: Snapshot + 回放后续事件 → 最终一致                │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### TCP 隧道流程

```
用户连接 :54001          服务端                    客户端
    │                    │                          │
    │──────────────────> │                          │
    │                    │──tunnel_request(WS)────> │
    │                    │                          │──连接 tunnel:9908 + token
    │                    │<──ACK──────────────────  │
    │                    │──配对──────────────────>  │
    │<═══════════════════╪══════════════════════════>│  双向 io.Copy
    │                    │                          │
```

## CLI 工具

```bash
# 认证
bbgrid-cli login -server https://xxx.xxx.xxx.xxx:9909 -api-key "your-api-key"
bbgrid-cli ping
bbgrid-cli status

# 插件
bbgrid-cli sync
bbgrid-cli run <action> [--key=value | value]

# 客户端
bbgrid-cli node list
bbgrid-cli node info my-device

# 注册管理
bbgrid-cli register apply -id my-device -pubkey my.pub -token <token>
bbgrid-cli register approve -id my-device -namespace permanent -role permanent
bbgrid-cli register revoke -id my-device -prefix <cert-prefix>
bbgrid-cli register pending
bbgrid-cli register list

# 代理管理
bbgrid-cli proxy list
bbgrid-cli proxy create -id my-device -remote 8080 -local 8080 -protocol tcp
bbgrid-cli proxy close 8080

# 中继管理
bbgrid-cli relay create -source client-A -target client-B -source-port 8090 -target-port 80
bbgrid-cli relay list
bbgrid-cli relay close <session-id>

# 命名空间
bbgrid-cli namespace list
bbgrid-cli namespace info permanent
bbgrid-cli namespace clients permanent
bbgrid-cli namespace assign -id my-device -namespace permanent -role permanent
```

## API 接口

| 方法 | 路径 | 认证 | 说明 |
|------|------|------|------|
| `GET` | `/PING` | 无 | 健康检查 |
| `GET` | `/status` | 无 | 服务器状态 |
| `GET` | `/health` | 无 | 健康检查 |
| `POST` | `/api/v1/auth/login` | API Key | 登录获取 JWT Token |
| `GET` | `/api/v1/sync` | 无 | 同步插件能力 |
| `POST` | `/api/v1/run` | 无 | 执行插件能力 |
| `POST` | `/api/v1/register/apply` | 无 | 提交注册申请 |
| `GET` | `/api/v1/register/list` | 无 | 查看已通过列表 |
| `POST` | `/api/v1/register/approve` | JWT | 审核通过并签发证书 |
| `POST` | `/api/v1/register/revoke` | JWT | 吊销客户端证书 |
| `GET` | `/api/v1/register/pending` | JWT | 查看待审核列表 |
| `GET` | `/api/v1/nodes` | JWT | 列出所有客户端 |
| `GET` | `/api/v1/nodes/:id` | JWT | 获取客户端详情 |
| `GET` | `/api/v1/proxies` | JWT | 列出所有代理 |
| `POST` | `/api/v1/proxies` | JWT | 创建代理映射 |
| `DELETE` | `/api/v1/proxies/:port` | JWT | 关闭代理 |
| `POST` | `/api/v1/relay` | JWT | 创建中继连接 |
| `GET` | `/api/v1/relay` | JWT | 列出中继会话 |
| `DELETE` | `/api/v1/relay/:id` | JWT | 关闭中继会话 |
| `GET` | `/api/v1/namespaces` | JWT | 列出所有命名空间 |
| `GET` | `/api/v1/namespaces/:name` | JWT | 获取命名空间详情 |
| `GET` | `/api/v1/namespaces/:name/clients` | JWT | 获取命名空间下的客户端 |
| `POST` | `/api/v1/namespaces/assign` | JWT | 设置客户端命名空间 |

## 项目结构

```
BBgrid/
├── BBgrid_Server/              # 服务端
│   ├── main.go                 # 入口
│   ├── bus.go                  # 消息总线 + Dispatcher
│   ├── supervisor.go           # 进程监控
│   ├── status.go               # 状态模板
│   └── workers/                # Worker 模块
│       ├── types.go            # 接口定义 (Bus, Dispatcher)
│       ├── auth.go             # 认证 Worker
│       ├── state.go            # 状态机 Worker (含持久化)
│       ├── data.go             # 数据面 Worker
│       ├── control.go          # 控制器 Worker
│       ├── ws.go               # WebSocket Worker
│       ├── conn.go             # 连接封装
│       ├── action_handler.go   # Action 处理器
│       └── middleware.go       # JWT 中间件
├── BBgrid_Client/              # 客户端
│   ├── main.go                 # 入口
│   ├── client.go               # 客户端核心
│   ├── conn/                   # WebSocket 连接
│   └── handler/                # 消息处理
├── BBgrid_Cmd/                 # 命令行工具
│   └── bbgrid-cli/             # CLI 管理工具
├── BBgrid_Runtime/             # 运行时 (WireGuard 管理)
│   ├── main.go                 # 入口 + Reconcile Loop
│   └── runtime.go              # WireGuard 接口管理
├── common/                     # 共享代码
│   ├── config/                 # 配置管理
│   ├── log/                    # 日志
│   ├── middleware/              # HTTP 中间件 (认证)
│   ├── model/                  # 数据模型
│   ├── mux/                    # 多路复用
│   ├── persist/                # 持久化 Provider 接口
│   ├── plugin/                 # 插件接口 + 能力注册表
│   ├── proto/                  # 协议定义 (GenericEvent, ResourceKey)
│   ├── relay/                  # 中继协议
│   ├── runtime/                # Runtime gRPC 服务
│   ├── sdk/                    # 插件 SDK
│   ├── store/                  # 持久化存储 (Event/Snapshot/Meta)
│   └── wsconn/                 # WebSocket 适配
├── plugins/                    # 插件目录
│   ├── latency/                # 延迟监控插件
│   ├── persist/                # 状态持久化插件
│   └── tag/                    # 标签管理插件
├── examples/                   # 示例配置
│   ├── server.json.example     # 服务端配置模板
│   ├── client.json.example     # 客户端配置示例
│   ├── env.example             # 客户端环境变量示例
│   └── cli.config.example.json # CLI 配置示例
├── docker/                     # Docker 构建与部署
├── docs/                       # 文档
│   └── deploy.md               # 部署手册
└── Makefile                    # 编译脚本
```

## License

Apache License 2.0 — 详见 [LICENSE](LICENSE) 文件
