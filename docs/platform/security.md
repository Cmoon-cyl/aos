# AOS 0.2 安全边界

当前实现已启用 TLS，并提供证书身份、有限 ACL 编译和受控 Router。本文说明安全责任和部署检查方法；使用入口见[安全 CLI](security-cli.md)，每机 peer/Router 扩展见[部署拓扑](topology.md)。具体源码和制品的通过范围以[发行说明](../releases/v0.2.0.md)和对应报告为准。

## 安全拓扑

最初的安全拓扑是 Node 以 Zenoh **client** 连接同一受控 Router。它保留为兼容入口：应用无 listener，关闭 multicast/gossip 与自动连接，只允许指定的 TLS Router。Router 开启双向 TLS、证书校验与默认拒绝 ACL，没有未认证的 TCP 备用监听。

当前也支持显式 TLS peer、本机 gossip 和多 Router 连接。peer listener 与本机 Router endpoint 限定 loopback IP，直接连接同样执行 mTLS/ACL；Router 转发权限由 `relays` 和 `forward_subjects` 声明，不能从原始主体任意扩权。下游识别相邻 Router，原始调用者身份不自动透传。具体约束见[topology.md](topology.md)。

QUIC、跨组织身份联邦与任意 Router mesh 高可用不在当前承诺内。开发 TCP 和诊断隧道不能作为安全模式失败后的备用连接。测试必须验证未声明的绕过路径被拒绝或不能通信，不能只检查配置内容。

## 身份如何成立

固定 Zenoh 1.10.1 源码从已建立 TLS 链路的对端 leaf 证书提取 CN，生成 `LinkAuthId::Tls`，ACL interceptor 再匹配 `cert_common_names`。可用字段和代码路径见[固定来源](sources.md#zenoh-身份与权限)。本期使用独立 CA 签发的证书，CN 作为部署主体标识；证书合法性、服务端主机名验证与 ACL 授权是三个独立结果。

| 数据 | 本期用途 | 不能证明 |
|---|---|---|
| 经过 mTLS 校验的证书 CN | router 上的 ACL subject | 原始业务调用者身份自动透传至 provider |
| 服务端证书 SAN/名称校验 | 客户端确认所连 router | 客户端对任意业务端点有权限 |
| ZID、Node 名、domain、namespace、NodeCard | 路由、发现、显示、选择资源 | 认证主体 |
| JSON `caller`、request_id、goal_id | 业务数据或诊断关联 | 获得权限、goal 所有权 |

配置使用真实字段 `transport.link.tls.root_ca_certificate`、`listen_certificate/listen_private_key`、`connect_certificate/connect_private_key`、`enable_mtls=true`、`verify_name_on_connect=true`、`close_link_on_expiration=true`。只显示路径及诊断状态；私钥不进入 NodeCard、bag、日志或跨机测试证据。

CN 必须非空，由部署者在所信任 CA 内唯一分配。两个获同一 CN 的证书会被视作同一主体；本期不额外承诺证书指纹级 ACL。证书过期后的连接关闭要实测；撤销通过更新信任/ACL、断开受影响现存连接并验证旧凭证失效完成，不宣称已有 CRL/OCSP 或无缝热更新能力。更新失败保持拒绝策略，不能降级为明文。

## 资源和操作的授权

策略以 remapping 后的实际 AOS key 编译，数据资源与发现资源分别控制。现有数据 key 为 `aos/<domain>/<expanded-name>/<type-name>/<type-hash>`；发现 token 位于 `aos/@lv/`，编码了 Node/实体身份、名称和类型。不得只保护 `aos/<domain>/**` 而遗漏发现、描述和内部 Action 实体。

| AOS 能力 | 需要核验的 Zenoh 消息类别 | 资源覆盖 |
|---|---|---|
| Topic 发布 | `put` | 目标 Topic 的具体名称、类型及哈希范围 |
| Topic 订阅、bag record | `declare_subscriber`，及接收路径上的 `put` | 被允许读取的 Topic |
| Service/Action 请求 | `query` | 精确服务名；Action 三个 Service 分开 |
| Service 注册与回复 | `declare_queryable`、`reply` | 被允许提供服务的端点 |
| Node/实体注册 | `liveliness_token` | 允许注册的 namespace/实体声明 |
| Graph 发现 | `liveliness_query`、`declare_liveliness_subscriber` | 被允许查看的发现范围 |
| NodeCard/Interface 描述 | `query/reply` 及所需注册 | 实际私有描述端点，不能当成免鉴权接口 |
| 参数修改、lifecycle 转换 | 对相应私有服务的 `query` | 与只读 get/list/describe 权限分开 |
| bag play | 与普通 Topic 发布相同 | 回放后 remapping 的实际资源；不因源文件可信而豁免 |

router 的 ingress/egress 规则共同保证结果；部署时需要用实际请求检查每类操作，不能只检查 `put`。`delete` 默认拒绝，除非有明确平台用途再单独授权。subject 不使用空组合通配；缺失、未知或不匹配 CN 不能因匿名兜底规则获得权限。

先提供有限角色：provider 可注册/发布/回复自己清单内能力；consumer 可发现/读取/调用被授权端点；operator 单独获得 param set、lifecycle change 和指定 Action cancel 权限。角色名是 AOS 配置概念，最终必须编译为固定 Zenoh 支持的 subject/rule/policy，不虚构 Zenoh 原生角色 API。

Action cancel 是端点/操作授权：某主体获准访问一个 Action 的 cancel 服务后，可能按协议选择该 Action 的其他 goal。**本期不承诺“只能取消自己提交的 goal”。** `goal_id` 难猜、payload 自报 caller、不同 Node 名都不能补齐这个保证。如未来增加 owner 隔离，必须先实现 provider 可校验的可信身份传递、owner 持久/生命周期规则及负向验收，再改变 API。

## 部署验证

| 场景 | 可判通过的结果 |
|---|---|
| 正确证书及允许角色 | TLS 建连、实际授权操作和业务结果都通过 |
| 无证书、错误 CA、过期证书、错误服务端名称 | 握手/连接失败，不能回退 TCP |
| 正确 CA 但未知/缺失 CN | 可按 TLS 行为建立链路，但未获任何业务权限；实际受限操作失败 |
| 假 Node 名、伪造 caller、另一个 domain/namespace | 不扩大证书主体原有权限 |
| 未授权 pub/query/cancel/param/lifecycle/注册/发现 | 每项分别拒绝；合法对应操作同时成功 |
| 未声明的直接 peer、旧 TCP 端点或额外 Router 路径 | 无法形成可绕过本策略的有效业务链路；获准 peer/relay 路径仍须执行相应 ACL |
| 凭证更新/撤销 | 新主体按策略工作；旧连接和旧凭证不继续保有被撤销权限 |
| 断网、router/provider 重启 | 重新认证、重新发现、重新就绪；无自动业务重放或虚构终态 |

若 Zenoh 的拒绝表现为丢弃/超时，CLI 报实际可知结果；router 侧拒绝日志作为授权拒绝证据。不能要求客户端凭 timeout 猜中拒绝原因。证据保留源码 SHA、构建 features、router 版本、非秘密配置、证书有效期/主体摘要、路径、正反例请求与最终状态；不用真实私钥作为附件。
