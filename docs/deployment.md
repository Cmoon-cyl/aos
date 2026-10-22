# 部署 AOS

默认部署单位是一台机器上的 Router 与若干应用 Node。
先验证单机实际业务，再连接另一台机器；跨不可信网络使用 mTLS 与明确的 ACL。

## 选择部署方式

| 场景 | 入口 | 需要准备 |
| --- | --- | --- |
| 本机试用 | 包内 `router/zenohd` 与开发配置 | CPython 3.11、完整平台包 |
| 可信 LAN 开发 | 每机 Router，显式连接另一台 Router | 对端实际 IP、端口和网络可达性 |
| 多地点或不可信网络 | `aos security serve`，TLS peer 与 Router federation | 可达入口、证书、有限权限策略、relay 授权 |
| 一组本机应用 | `aos run` / `aos launch` | 安装前缀、进程入口、readiness 与有限 restart policy |
| Linux 常驻应用 | systemd 用户服务 | 已验证的安装前缀和服务配置 |

## 单机

按[安装指南](install.md)启动 Router。SDK/CLI 默认以 peer 模式连接
`tcp/localhost:7447`，监听本机临时端口；组播关闭，gossip 开启。
应用可以先于 Router 启动并在后台重连，Context 打开成功不代表业务已经就绪。

开发 Router 默认监听网络接口：macOS/Linux 使用 `[::]:7447`，Windows 使用
`0.0.0.0:7447`。只需要本机访问时，可在自己复制的 Router 配置中把 listener 改为
`tcp/127.0.0.1:7447`。修改配置后重新启动该 Router，再验证客户端。

本机 peer 通过 gossip 发现可达邻居并建立直接连接；已经建立的直接连接可在 Router 暂时退出后继续通信。
新的 peer 仍需要 Router 或显式入口来获得发现信息。

## 两台机器的开发连接

两台机器分别启动本机 Router。在其中一台 Router 配置的
`connect.endpoints` 中填入对端可达地址，例如：

```json5
connect: {
  endpoints: ["tcp/192.168.1.10:7447"],
},
```

这是配置片段，应合并到完整 Router 配置，不能直接当作完整 session 文件。
示例 IP 必须替换为实际地址。只需一端主动连接，建立的链路可以承载双向业务。
两端应用使用一致的 domain，分别连接本机 Router。

验收时同时记录请求端和 provider 端的实际值。至少检查 NodeCard 身份、
Topic 收到的内容、Service 计算结果、Action 成功与取消终态，随后反转调用方向。
端口可达与 Graph 可见分别只证明相应步骤。

## 跨网与 mTLS

gossip 不负责 NAT 穿透，也不会自动打开防火墙或推导公网地址。
多地点部署需要一个对端可达的入口；主动连接方可以位于 NAT 后，连接后仍可承载双向业务。
网络入口与映射由部署者配置，AOS 不修改系统网关、防火墙或 NAT。

[TLS peer 与每机 Router](platform/topology.md#使用-tls-peer-与每机-router)
给出完整 profile 结构与生成方式：

1. 为应用和 Router 分配证书身份。SAN 覆盖实际连接地址；peer/Router 证书同时满足客户端与服务端用途。
2. 编写实际业务的有限 ACL。发现、描述、请求、注册、结果与取消分别授权。
3. 每个 peer 显式连接自己的 loopback TLS Router，并列出可信本机 relay。
4. Router 显式连接对端 Router，双方通过 `forward_subjects` 声明有限转发权限。
5. 运行 `aos security serve --request /absolute/router-request.json`。
6. 使用各自的 `security_profile` 启动应用，验证获准正例、拒绝边界和真实业务结果。

`router_ready` 只表示 listener 初始化。下游 Router 看到的是相邻 Router 的证书身份，
因此 Router 属于部署信任边界；多 Router 转发不提供端到端原始调用者身份透传。
客户端发生配置或握手失败时不会自动退回 TCP。

## 启动与管理应用

使用[安装包索引](platform/installed-packages.md)固定入口和资源，
通过[run/launch](platform/process-cli.md)声明配置、依赖、就绪和重启次数。
readiness 核对本次启动的进程身份，不能只匹配 Node 名称。
需要安全控制时，supervisor 与各 child 显式使用各自 profile。

Linux 的系统服务入口见[systemd 用户服务](platform/system-service.md)。
普通 `run/launch` 负责当前调用拥有的进程；它不是开机自启配置，也不替业务恢复持久状态。
Windows 服务与 macOS launchd 的系统集成不由 Linux systemd 工具提供。

## 升级与恢复

在新目录和新 venv 安装同一发行版的 CLI/wheel，保留原配置和业务数据。
先完成本机业务检查，再切换跨机入口；使用 [0.2 迁移指南](platform/migration-0.2.md)
检查默认拓扑、配置优先级和 CLI 行为变化。

Router/provider 重启后的发现与连接恢复，不会自动重放 Service 或恢复旧 Action goal。
已有写操作需要业务幂等与结果查询；已有完整文件按应用存储契约保留。
更换网络、证书或 Router 拓扑后，应重新检查双向发现、实际业务和故障恢复。
