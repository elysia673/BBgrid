# BBgrid

模块化网络代理平台，通过插件系统组装你需要的架构

## 特性

- **插件驱动**: Core + 插件积木，按需组装架构
- **事件总线**: 基于 ResourceKey 的 EventBus 分发，Core 不理解业务逻辑
- **能力协商**: 插件启动时注册 Capability，即插即用
- **Provider 体系**: Proxy/Relay 作为独立 Provider 注册到 CapabilityRegistry
- **期望状态引擎**: ReconcileEngine 双向同步 desired ↔ actual，自动创建/删除
- **事件驱动触发**: proxy/relay 变化立即触发 Reconcile，不只靠定时器
- **失败退避**: 创建/删除失败时指数退避（30s → 60s → 120s ... 最大 10min），避免重连风暴
- **持久化层**: EventStore + SnapshotStore + MetaStore，支持崩溃恢复
- **中继转发**: 源端 → relay server → 目标端，mux + net.Pipe 隔离 + io.Copy + 半关闭
- **自动恢复**: 重启后 proxy/relay 自动恢复，客户端重连自动补发信号
- **Namespace 隔离**: permanent / temporary / mediated 三种命名空间
- **安全认证**: mTLS + JWT Token + API Key 三重认证
- **守护进程**: bbgrid-daemon 自动拉起和监控服务
- **进程管理**: bbgrid-ctl 统一管理所有服务

## 快速开始

### 1. 编译

```bash
make build          # server + client + cli + ctl + daemon + runtime
make server         # 只编译 server
make client         # 只编译 client
make cli            # 只编译 cli
make ctl            # 只编译 ctl
make daemon         # 只编译 daemon
make check          # 格式化 + vet + 测试
```

### 2. 配置

```bash
mkdir -p config

# 生成配置文件
bbgrid-ctl init server
bbgrid-ctl init client
```

### 3. 启动 Daemon

```bash
# 创建 daemon 配置
cat > config/daemon.json << EOF
{
  "socket_path": "/var/run/bbgrid/daemon.sock",
  "log_path": "/var/log/bbgrid/daemon.log",
  "ctl_path": "/usr/local/bbgrid/bin/bbgrid-ctl",
  "server": {
    "enabled": true,
    "bin_path": "/usr/local/bbgrid/bin/bbgrid-server",
    "config_path": "/usr/local/bbgrid/config/server.json"
  },
  "client": {
    "enabled": true,
    "bin_path": "/usr/local/bbgrid/bin/bbgrid-client",
    "config_path": "/usr/local/bbgrid/config/client.json"
  }
}
EOF

# 启动 daemon
bbgrid-daemon -config config/daemon.json
```

### 4. 管理服务

```bash
# 查看状态
bbgrid-ctl status

# 启动/停止/重启
bbgrid-ctl start server
bbgrid-ctl stop client
bbgrid-ctl restart all
```

## 架构

### 守护进程架构

```
┌─────────────────────────────────────────────────────────────────┐
│                      bbgrid-daemon                              │
│                      (常驻进程)                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │                    Unix Socket                            │ │
│  │              /var/run/bbgrid/daemon.sock                   │ │
│  └───────────────────────────────────────────────────────────┘ │
│         ▲                    ▲                    ▲             │
│         │ register           │ command            │ status      │
│         │ heartbeat          │                    │             │
│         │                    │                    │             │
│  ┌──────┴──────┐   ┌────────┴────────┐   ┌──────┴──────┐      │
│  │   Service   │   │   bbgrid-ctl    │   │   Query     │      │
│  │   Manager   │   │   (CLI 工具)    │   │   (查询)    │      │
│  └──────┬──────┘   └────────┬────────┘   └─────────────┘      │
│         │                    │                                  │
│         ▼                    ▼                                  │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │                    Process Manager                        │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐  │ │
│  │  │   start     │  │    stop     │  │    restart      │  │ │
│  │  │   通过ctl   │  │   通过ctl   │  │    通过ctl      │  │ │
│  │  └─────────────┘  └─────────────┘  └─────────────────┘  │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │                    Health Check                           │ │
│  │  ┌─────────────────────────────────────────────────────┐ │ │
│  │  │  每5秒检查一次                                        │ │ │
│  │  │  ├─ 服务应该运行但没有运行 → 重新拉起                   │ │ │
│  │  │  └─ 服务正在运行但进程已死 → 重新拉起                   │ │ │
│  │  └─────────────────────────────────────────────────────┘ │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
         │                              │
         │ 拉起/停止                     │ 拉起/停止
         ▼                              ▼
┌──────────────────────┐   ┌──────────────────────┐
│   bbgrid-server      │   │   bbgrid-client      │
│   (服务端)           │   │   (客户端)           │
├──────────────────────┤   ├──────────────────────┤
│  - 注册到 daemon     │   │  - 注册到 daemon     │
│  - 发送心跳          │   │  - 发送心跳          │
│  - 处理代理/中继     │   │  - 建立隧道          │
└──────────────────────┘   └──────────────────────┘
```

### 启动流程

```
daemon 启动
    │
    ▼
监听 Unix Socket
    │
    ▼
通过 ctl 拉起 server
    │
    ▼
exec("bbgrid-ctl", "-socket", "/var/run/bbgrid/daemon.sock", "start", "server")
    │
    ▼
ctl 连接 daemon socket
    │
    ▼
daemon 收到命令，拉起 server
    │
    ▼
server 启动，注册到 daemon
    │
    ▼
daemon 监控 server 状态
```

### 健康检查流程

```
健康检查循环 (每5秒)
    │
    ▼
遍历所有服务
    │
    ├─ 服务应该运行但没有运行
    │   │
    │   ▼
    │   通过 ctl 重新拉起
    │
    └─ 服务正在运行但进程已死
        │
        ▼
        通过 ctl 重新拉起
```

### 退出流程

```
daemon 收到退出信号
    │
    ▼
通过 ctl 停止 server
    │
    ▼
通过 ctl 停止 client
    │
    ▼
关闭 socket
    │
    ▼
daemon 退出
```

### 认证流程

```
客户端连接
    │
    ├─ 有证书 (mTLS)
    │   └─ 可以注册 + 使用代理/中继
    │
    └─ 无证书 (Token)
        └─ 只能注册，等待签发证书
```

### 整体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                      HTTP :9909 (Control Plane)                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────────┐    │
│  │   EventBus   │  │  StateStore  │  │ CapabilityRegistry│    │
│  │ (事件分发器)  │  │ (runtime +   │  │ (Provider + Action)│   │
│  │              │  │  desired)    │  │                   │    │
│  └──────┬───────┘  └──────────────┘  └───────────────────┘    │
│         │                                                       │
│         ▼                                                       │
│  ┌─────────────────────────────────────────────────────┐      │
│  │  ReconcileEngine (期望状态引擎)                       │      │
│  │  ┌────────────┐  ┌────────────┐  ┌──────────────┐  │      │
│  │  │ 双向同步    │  │ 事件触发    │  │ 失败退避     │  │      │
│  │  │ create +   │  │ ADDED/     │  │ 30s→60s→...  │  │      │
│  │  │ delete     │  │ DELETED    │  │ max 10min    │  │      │
│  │  └────────────┘  └────────────┘  └──────────────┘  │      │
│  └─────────────────────────────────────────────────────┘      │
│                                                                 │
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
│                      │  │  客户端重连补发            │
│  proxy    代理 Provider│  │                          │
│  relay    中继 Provider│  │                          │
│                      │  │                          │
└──────────────────────┘  └──────────────────────────┘
```

## 组件说明

### bbgrid-daemon

守护进程，负责：
- 启动时通过 ctl 拉起 server/client
- 健康检查，服务死亡自动重启
- 接收 server/client 注册和心跳
- 退出时通过 ctl 停止服务

### bbgrid-ctl

进程管理工具，负责：
- start/stop/restart 服务
- 查看服务状态
- 生成配置文件

### bbgrid-server

服务端，负责：
- 管理客户端连接
- 处理代理/中继请求
- 持久化状态

### bbgrid-client

客户端，负责：
- 连接服务器
- 建立隧道
- 转发数据

## 配置文件

### daemon.json

```json
{
  "socket_path": "/var/run/bbgrid/daemon.sock",
  "log_path": "/var/log/bbgrid/daemon.log",
  "ctl_path": "/usr/local/bbgrid/bin/bbgrid-ctl",
  "server": {
    "enabled": true,
    "bin_path": "/usr/local/bbgrid/bin/bbgrid-server",
    "config_path": "/usr/local/bbgrid/config/server.json"
  },
  "client": {
    "enabled": true,
    "bin_path": "/usr/local/bbgrid/bin/bbgrid-client",
    "config_path": "/usr/local/bbgrid/config/client.json"
  }
}
```

### server.json

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

### client.json

```json
{
  "server_url": "wss://your-server:9909/ws",
  "client_id": "my-device",
  "client_token": "your-client-token",
  "data_dir": "./data",
  "log_path": "./data/client.log"
}
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
│   ├── session/                # Session Layer (WS + 隧道 + 中继桥接)
│   ├── dataplane/              # Data Plane (Tunnel :9908)
│   └── ssl/                    # TLS 证书
├── BBgrid_Client/              # 客户端
│   ├── main.go                 # 入口
│   ├── sdk/                    # SDK 层
│   ├── client/                 # 客户端核心
│   ├── transport/              # 传输层
│   ├── tunnel/                 # 隧道管理
│   ├── relay/                  # 中继管理
│   ├── api/                    # API 客户端
│   └── platform/               # 平台适配
├── BBgrid_Cmd/bbgrid-cli/      # CLI 工具
├── BBgrid_Ctl/                 # 进程管理工具
│   ├── main.go                 # 入口
│   ├── cmd.go                  # 命令处理
│   └── internal/
│       ├── client/             # daemon 客户端
│       ├── process/            # 进程管理
│       ├── update/             # 更新管理
│       └── config/             # 配置管理
├── BBgrid_Daemon/              # 守护进程
│   ├── main.go                 # 入口
│   └── internal/
│       └── daemon/             # daemon 核心
├── BBgrid_Runtime/             # 运行时 (WireGuard，待实现)
├── common/                     # 共享代码
│   ├── config/                 # 配置管理
│   ├── log/                    # 结构化日志
│   ├── model/                  # 数据模型
│   ├── mux/                    # TCP 多路复用
│   ├── persist/                # 持久化 Provider 接口
│   ├── plugin/                 # 插件接口
│   ├── proto/                  # GenericEvent + ResourceKey
│   ├── store/                  # EventStore + SnapshotStore + MetaStore
│   ├── daemon/                 # daemon 客户端库
│   ├── pidfile/                # PID 文件管理
│   └── wsconn/                 # WebSocket → net.Conn 适配
├── plugins/                    # 插件目录
│   ├── latency/                # 延迟监控
│   ├── persist/                # 状态持久化
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
