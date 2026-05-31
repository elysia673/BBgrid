# BBgrid

模块化网络代理平台，通过插件系统组装你需要的架构

## 特性

- **插件驱动**: Core + 插件积木，按需组装架构
- **事件总线**: 基于 ResourceKey 的 EventBus 分发，Core 不理解业务逻辑
- **能力协商**: 插件启动时注册 Capability，即插即用
- **Provider 体系**: Proxy/Relay 作为独立 Provider 注册到 CapabilityRegistry
- **持久化层**: EventStore + SnapshotStore + MetaStore，支持崩溃恢复
- **最终一致**: Reconcile Loop 自动同步状态
- **多协议支持**: TCP、UDP、WebSocket 隧道
- **中继转发**: 源端 → relay server → 目标端，全链路 io.Copy + 半关闭
- **Namespace 隔离**: permanent / temporary / mediated 三种命名空间
- **安全认证**: mTLS + JWT Token + API Key 三重认证
- **Docker 部署**: 自签名/本地证书两种部署脚本

## 快速开始

### 1. 编译

```bash
make build          # server + client + cli + runtime
make server         # 只编译 server
make client         # 只编译 client
make cli            # 只编译 cli
make check          # 格式化 + vet + 测试
```

### 2. 配置 Server

```bash
mkdir -p data
cp server_config.example conf/config.json
vim conf/config.json
```

关键配置项：
```json
{
  "addr": ":9909",
  "domain": "your-domain.com",
  "tunnel_port": 9908,
  "public_ip": "your-public-ip",
  "data_dir": "/path/to/data",
  "log_path": "/path/to/data/server.log",
  "api_key": "your-api-key",
  "client_token": "your-client-token",
  "tls_cert": "/path/to/cert.pem",
  "tls_key": "/path/to/key.pem"
}
```

### 3. 启动 Server

```bash
./bin/bbgrid-server -config conf/config.json
```

### 4. CLI 登录

```bash
bbgrid-cli login -server your-server:9909 -api-key "your-api-key"
bbgrid-cli sync
bbgrid-cli status
```

### 5. 部署客户端

```bash
./bin/bbgrid-client -config client.json
```

### 6. 使用

```bash
# 查看节点
bbgrid-cli node list

# 创建代理
bbgrid-cli proxy create test -remote 8080 -local 80

# 创建中继
bbgrid-cli relay create test my_device -aport 9091 -bport 22

# 插件操作
bbgrid-cli run tag.set client_id=test env=prod
bbgrid-cli run latency.list
```

## 架构

### 整体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                      HTTP :9909 (Control Plane)                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────────┐    │
│  │   EventBus   │  │  StateStore  │  │ CapabilityRegistry│    │
│  │ (事件分发器)  │  │ (状态接口)    │  │ (Provider + Action)│   │
│  └──────┬───────┘  └──────────────┘  └───────────────────┘    │
│         │                                                       │
│         ▼                                                       │
│  ┌─────────────────────────────────────────────────────┐      │
│  │              StorageManager (持久化层)                │      │
│  │  EventStore  │  SnapshotStore  │  MetaStore          │      │
│  └─────────────────────────────────────────────────────┘      │
│                                                                 │
│  HTTP :9909              Tunnel :9908                           │
└─────────────────────────────────────────────────────────────────┘
         │                              │
         │ 插件自动注册                   │ 数据面
         ▼                              ▼
┌──────────────────────┐  ┌──────────────────────────┐
│    官方插件 (内置)     │  │      Session Layer       │
├──────────────────────┤  ├──────────────────────────┤
│                      │  │                          │
│  latency  延迟监控    │  │  WS 连接管理              │
│  persist  状态持久化  │  │  实时推送                  │
│  tag      标签管理    │  │  隧道配对                  │
│  file     文件传输    │  │  中继 WS 桥接             │
│                      │  │                          │
│  proxy    代理 Provider│  │                          │
│  relay    中继 Provider│  │                          │
│                      │  │                          │
└──────────────────────┘  └──────────────────────────┘
```

### 插件化 Proxy/Relay

Proxy 和 Relay 作为独立插件注册到 CapabilityRegistry：

```
API 请求 → EventBus → 插件 Provider (listener 管理)
                     → Session Layer (隧道桥接 + WS 通知)
                     → StateStore (状态更新)
                     → Core (持久化)
```

**职责分离：**
- **proxy 插件**: 管理 TCP listener 生命周期 (Create/Delete)
- **relay 插件**: 管理中继信号通知 (通过 notifyFn)
- **Session Layer**: 接受隧道连接 + relay WS 配对 + 数据桥接

### 启动流程

```
main.go:
  NewCore()                    ← 创建 EventBus + StateStore
  session.NewServer()          ← 不订阅 EventBus
  initPlugins()                ← 插件 Init 注册 Provider + 订阅 EventBus
  sess.StartEventSubscriptions() ← 注入 notifyFn + 订阅 EventBus
  core.Start()                 ← 启动 ReconcileEngine
```

### 恢复流程

```
persist.Run():
  ├─ restoreFromMetaStore()  → EventBus ADDED → StateStore + Session
  ├─ restoreSnapshot()       → StateStore.Restore() (desired state)
  └─ restoreAll()            → 插件状态 (tag)

客户端重连:
  trackSession()
  ├─ syncClientProxies()     → 补发 proxy 配置
  ├─ syncClientRelays()      → 从 StateStore 补发 relay 信号
  └─ deliverPendingRelaySignals() → 补发暂存信号
```

### TCP 隧道流程

```
用户连接 :54001       Server                    Client
    │                 │                          │
    │───────────────> │                          │
    │                 │──tunnel_request(WS)────> │
    │                 │                          │──连接 tunnel:9908 + token
    │                 │<──ACK──────────────────  │
    │                 │──配对──────────────────>  │
    │<════════════════╪══════════════════════════>│  io.Copy
    │                 │                          │
```

### 中继流程

```
CLI: relay create A B -aport 9091 -bport 22
  → Server 创建 relay session
  → 通知 A (source) 和 B (target)

A (source):                        B (target):
  连接 relay WS                      连接 relay WS
  创建 mux                           创建 mux
  监听 9091                          设置 LocalTarget=127.0.0.1:22

用户 SSH -p 9091 127.0.0.1:
  A accept → mux channel → relay WS → server 桥接 → target WS → mux channel → B dial 127.0.0.1:22
  双向 io.Copy + 半关闭
```

## CLI 工具

```bash
# 认证
bbgrid-cli login -server https://server:9909 -api-key "key"
bbgrid-cli ping
bbgrid-cli status

# 插件
bbgrid-cli sync
bbgrid-cli run <action> [--key=value | value]

# 节点
bbgrid-cli node list
bbgrid-cli node view <id>

# 注册
bbgrid-cli register apply -id my-device -pubkey my.pub -token <token>
bbgrid-cli register approve -id my-device -namespace permanent -role permanent
bbgrid-cli register revoke -id my-device
bbgrid-cli register pending
bbgrid-cli register list

# 代理
bbgrid-cli proxy list
bbgrid-cli proxy create <client_id> -remote 8080 -local 80
bbgrid-cli proxy close <port>

# 中继
bbgrid-cli relay create <A> <B> -aport 9091 -bport 22
bbgrid-cli relay list
bbgrid-cli relay close <session-id>

# 命名空间
bbgrid-cli namespace list
bbgrid-cli namespace info <name>
bbgrid-cli namespace clients <name>
bbgrid-cli namespace assign -id my-device -namespace permanent -role permanent
```

## API 接口

| 方法 | 路径 | 认证 | 说明 |
|------|------|------|------|
| `GET` | `/PING` | 无 | 健康检查 |
| `GET` | `/status` | 无 | 服务器状态 |
| `GET` | `/health` | 无 | 健康检查 |
| `POST` | `/api/v1/auth/login` | API Key | 登录获取 JWT |
| `POST` | `/api/v1/register/apply` | 无 | 提交注册申请 |
| `GET` | `/api/v1/register/list` | 无 | 已通过列表 |
| `POST` | `/api/v1/register/approve` | JWT | 审核签发证书 |
| `POST` | `/api/v1/register/revoke` | JWT | 吊销证书 |
| `GET` | `/api/v1/register/pending` | JWT | 待审核列表 |
| `GET` | `/api/v1/nodes` | JWT | 所有客户端 |
| `GET` | `/api/v1/nodes/:id` | JWT | 客户端详情 |
| `GET` | `/api/v1/proxies` | JWT | 所有代理 |
| `POST` | `/api/v1/proxies` | JWT | 创建代理 |
| `DELETE` | `/api/v1/proxies/:port` | JWT | 删除代理 |
| `POST` | `/api/v1/relay` | JWT | 创建中继 |
| `GET` | `/api/v1/relay` | JWT | 中继列表 |
| `DELETE` | `/api/v1/relay/:id` | JWT | 关闭中继 |
| `GET` | `/api/v1/namespaces` | JWT | 命名空间列表 |
| `GET` | `/api/v1/namespaces/:name` | JWT | 命名空间详情 |
| `GET` | `/api/v1/namespaces/:name/clients` | JWT | 命名空间客户端 |
| `POST` | `/api/v1/namespaces/assign` | JWT | 分配命名空间 |
| `GET` | `/runtime/capabilities` | JWT | 插件能力列表 |
| `POST` | `/runtime/call` | JWT | 执行插件操作 |
| `GET` | `/runtime/query` | JWT | 查询状态 |
| `GET` | `/runtime/download` | JWT | 下载文件 |

## 项目结构

```
BBgrid/
├── BBgrid_Server/              # 服务端
│   ├── main.go                 # 入口 + 配置加载 + 插件初始化
│   ├── auth/                   # CA/JWT/注册表/命名空间
│   ├── http/                   # Gin HTTP 路由 + 中间件
│   ├── runtime/                # Core: EventBus + StateStore + Reconcile + Capability
│   │   ├── types.go            # 接口定义 (含 ProxyProvider/RelayProvider)
│   │   ├── eventbus.go         # 事件总线
│   │   ├── statestore.go       # 状态存储
│   │   ├── reconcile.go        # 协调引擎
│   │   └── capability.go       # 能力注册表
│   ├── session/                # Session Layer (WS + 隧道 + 中继桥接)
│   ├── dataplane/              # Data Plane (Tunnel :9908)
│   └── ssl/                    # TLS 证书
├── BBgrid_Client/              # 客户端
│   ├── main.go                 # 入口
│   ├── client.go               # WS 连接 + 注册
│   ├── conn/                   # WebSocket 连接封装
│   └── handler/                # 消息处理 + 隧道 + 中继
├── BBgrid_Cmd/bbgrid-cli/      # CLI 工具
├── BBgrid_Runtime/             # 运行时 (WireGuard，待实现)
├── common/                     # 共享代码
│   ├── config/                 # 配置管理
│   ├── log/                    # 结构化日志
│   ├── model/                  # 数据模型
│   ├── mux/                    # TCP 多路复用 (含测试)
│   ├── persist/                # 持久化 Provider 接口
│   ├── plugin/                 # 插件接口 + gRPC
│   ├── proto/                  # GenericEvent + ResourceKey
│   ├── relay/                  # NAT 穿透
│   ├── store/                  # EventStore + SnapshotStore + MetaStore
│   ├── util/                   # 工具函数 (PipeBidir)
│   └── wsconn/                 # WebSocket → net.Conn 适配
├── plugins/                    # 插件目录
│   ├── latency/                # 延迟监控
│   ├── persist/                # 状态持久化 (含 MetaStore 恢复)
│   ├── tag/                    # 标签管理
│   ├── file/                   # 文件传输
│   ├── proxy/                  # TCP Proxy Provider 插件
│   └── relay/                  # Relay Provider 插件
├── docker/                     # Docker 构建与部署
├── docs/                       # 文档
├── Makefile                    # 编译脚本
└── go.mod                      # Go 模块 (BBgrid)
```

## License

Apache License 2.0 — 详见 [LICENSE](LICENSE) 文件
