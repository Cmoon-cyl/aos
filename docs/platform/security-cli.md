# 安全 CLI 与 Python 入口

AOS 的安全模式使用 TLS、客户端证书和默认拒绝的 ACL。支持原有单 Router client，以及[每机 Router、本机 peer/gossip 和跨 Router 互联](topology.md)。证书的 Common Name 是认证主体；Node 名称、namespace、domain 和接口哈希是授权资源。修改 Node 名称不会获得另一主体的权限。

当前入口复用同一套 Rust profile 校验和策略编译器。Router 运行入口覆盖 macOS、Linux 和 Windows；具体候选的平台验收以对应报告为准。多 Router 高可用和在线策略热更新不在此入口的承诺范围内。

## 选择客户端配置

Python 使用 `aos.Context(security_profile="/absolute/client.json")`。CLI 在请求 JSON 中加入 `"security_profile": "/absolute/client.json"`。两者的显式参数优先于 `AOS_SECURITY_PROFILE`；没有显式参数时才读取环境变量。

客户端 profile 示例：

```json
{
  "schema_version": 1,
  "domain_id": 29,
  "router_endpoints": ["tls/router.example.test:7447"],
  "root_ca_certificate": "/absolute/certs/ca.pem",
  "certificate": "/absolute/certs/operator.pem",
  "private_key": "/absolute/certs/operator.key",
  "connect_timeout_ms": 3000
}
```

示例主机和路径需要替换为实际部署值。Router 证书的 SAN 必须覆盖连接主机，客户端证书的 Common Name 必须与策略主体相符。证书和私钥由部署方配置，CLI 不签发证书，也不复制凭据。

Profile 必须是非符号链接的普通文件，采用绝对 UTF-8 路径，大小不超过 1 MiB。空值、相对路径、缺失文件、未知字段、非法 JSON 或非法 TLS endpoint 均失败。已选择安全配置后，失败不会退回开发配置。

Profile 的 `domain_id` 是默认值。显式、环境及进程 bootstrap 的最终 domain 必须与它一致；最终原生配置必须与生成的安全配置一致，不能加入明文连接、额外 Router、发现路径或内联证书。客户端只使用 profile 指定的 CA 信任根，并验证 Router 名称和证书有效期。

需要创建 Node 的 CLI 操作还要提供自己的授权身份，例如：

```json
{
  "security_profile": "/absolute/client.json",
  "client_node": {"name": "operator", "namespace": "/secure"}
}
```

`client_node` 是 CLI 自己的 Node，独立于目标 provider。其身份及所需操作必须存在于该证书主体的策略中。只读取 Graph 的命令不需要创建 CLI Node，但证书仍需拥有相应 `graph_nodes` 权限。Graph 可见性、描述接口、Topic/Service 操作及 Action 的启动、结果、取消分别授权。

## 审阅与运行策略

策略结构见 [安全核心契约](security-runtime.md)。名称使用 remap 后的最终名称；接口提供完整 AOS Interface JSON。编译器推导实际 wire key、类型哈希和所需控制消息，调用方不能直接写原生通配 ACL。

以下三个 CLI 操作只校验和生成 JSON，不打开网络，也不写凭据或配置文件：

| 操作 | 请求字段 | 返回 |
| --- | --- | --- |
| `aos security compile --request policy-request.json` | `policy` | 默认拒绝的 ACL、展开授权理由、策略摘要 |
| `aos security router-config --request router-request.json` | `policy`、`router` | TLS Router 原生配置及编译结果 |
| `aos security client-config --request client-request.json` | `client` | TLS 客户端原生配置 |

Python 对应入口为 `compile_security_policy(policy)`、`router_security_config(router, policy)` 和 `client_security_config(client)`，参数为 Python 字典。文件内容与证书有效性在实际 TLS 打开时检验；离线编译成功不证明握手或授权成功。

`router` 使用 `schema_version`、`listen_endpoints`、`root_ca_certificate`、`certificate`、`private_key`。监听地址仅允许一个 `tls/host:port`。在请求中放入 `policy` 与 `router` 后运行：

```sh
aos security serve --request /absolute/router-request.json
```

该进程直接使用受控 Rust 配置打开 Router，不读取客户端安全 profile 或任意原生配置环境覆盖。`router_ready` JSONL 事件返回实际 ZID、TLS locators、domain 和策略摘要，只证明监听器已初始化。要验证部署，应继续运行获准的客户端握手及具体业务操作。

`startup_timeout` 默认 10 秒，`shutdown_timeout` 默认 5 秒，均要求大于 0 且不超过 60 秒。收到 SIGHUP、SIGINT、SIGQUIT 或 SIGTERM 后尝试关闭 native session，并输出一个 summary。关闭成功时按首个信号返回退出码：

| 信号 | 退出码 |
| --- | --- |
| SIGHUP | 129 |
| SIGINT | 130 |
| SIGQUIT | 131 |
| SIGTERM | 143 |

上述信号表适用于 macOS/Linux。Windows 使用控制台 Ctrl-C / Ctrl-Break 触发同一有界关闭流程，成功关闭后返回 130；强制终止进程不代表 native close 成功。

信号处理在请求和 profile 校验完成后安装，并保持到 `main` 的最终 JSONL 输出收尾结束；重复信号不再把收尾过程切回默认终止动作，也不改变已记录的首个信号。收尾共用最多 2 秒的输出预算。输出在该期限内恢复可写时，summary 与 error 按顺序排出；持续不可写或写入错误时，不能保证消费者收到最终记录。

只有 native close 成功时才返回 `session_closed: true`。open 线程创建失败、native open 失败或 open 结果通道断开，均以 `stage: "opening"`、`session_closed: false` 的 summary 记录启动失败，并返回 `ROUTER_START_FAILED`。启动中断、启动超时或关闭超时也会说明尚未确认的状态；CLI 退出后的 OS 资源回收不能代替 native close 证据。

## 进程启动与诊断

安全模式下，配置了 readiness 的 `aos run` / `aos launch` 使用自身的 profile 和 `client_node` 做检查。没有 readiness 时不创建 supervisor 控制会话，也不要求这个未使用的 Node 身份。每个子进程必须在自己的 `env` 中显式选择 `AOS_SECURITY_PROFILE`，不自动共用 supervisor 凭据。

所有子进程启动前，CLI 检查已知部署配置层与继承/显式环境：最终 domain 必须匹配该 child profile，最终 native 配置必须与它生成的安全配置相同，Router endpoint 必须与 supervisor profile 相同。应用在代码中自行提供的 program 配置无法提前预测，仍由 child 的标准 secure opener 最终校验。离线 preflight 不证明证书文件可加载、密钥匹配、证书链或有效期正确，这些检查交给实际连接的 native TLS 栈。

Python `aos.doctor_report(security_profile=...)` 与 CLI doctor 使用被选中的 profile。它们报告 TLS 配置，握手与 `authorization` 不作独立确认：peer Context 可以早于 Router 连接打开，连上 Router 也不证明某项业务操作获准。配置错误时，Graph 检查为 `not_run`，不会通过另一个开发连接补结果。

Zenoh 1.10.1 不向客户端提供独立的 ACL 拒绝错误。超时本身不能证明策略生效；拒绝验收需要关联 Router 的实际拒绝日志、目标业务没有变化以及授权正例。`security serve` 的 ready 事件和 doctor 均不宣称完成这项验收。
