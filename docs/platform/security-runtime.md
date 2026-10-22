# mTLS 与权限编译

安全配置用证书的 Common Name（CN）识别相邻连接主体，按编译后的默认拒绝 ACL 处理消息。原有 profile 把 client 固定连接到一台受控 TLS Router；显式 `peer` 与 `federation` 扩展支持[每机 Router 和同机 peer/gossip](topology.md)。Node 名、namespace、domain、请求 ID、goal ID 和消息正文都不是身份凭据。

这份契约适用于 `aos-zenoh::security` 的 v1 profile 和 policy。安全入口先完成进程 bootstrap 与 Context 配置合并，再检查最终 domain 和 Zenoh 配置；失败会返回错误，不会转用开发配置。

## 配置入口

`load_client_profile(Some(path))` 优先使用显式路径；没有显式路径时读取 `AOS_SECURITY_PROFILE`。两者均未配置才返回 `None`。已配置但为空、格式不合法、文件打不开或内容无效，都返回错误。文件必须是绝对 UTF-8 路径、普通文件、不超过 1 MiB；Unix 拒绝末级路径的符号链接与 FIFO，Windows 拒绝末级路径的 reparse point。读取和大小检查针对同一个已打开的文件描述符。

Python `Context` 在调用这个原生加载器前先完成来源选择：显式 `security_profile` 优先；否则同时检查 Python `os.environ` 和原生进程环境，保留任一来源的值，包括空值。两个来源都存在但路径值不一致时返回 `InvalidArgumentError`，不会选择普通 peer。SDK doctor 的来源核对和诊断边界见 [doctor 文档](../sdk-doctor.md)。

Profile 只携带 CA、证书和私钥的文件路径，不接收 PEM 或 base64 内容。TLS transport 在连接时加载凭据并检查其有效性。三个路径必须分别指定；profile 本身的 JSON 不允许重复键或未知字段。

| 约束 | v1 行为 |
| --- | --- |
| 原有 client 拓扑 | 一个 `tls/host:port` endpoint；不接受 endpoint 属性、额外 Router 或明文地址 |
| 客户端 | `mode=client`，双向认证，验证服务端名称，证书过期时断开连接 |
| Router | 只监听 TLS，验证客户端证书，按证书 CN 选择 ACL 主体 |
| 信任根 | 显式 CA 替代内置公共根；空显式 CA 不会回退 |
| 其他入口 | 原有 profile 禁用 multicast/gossip；显式 peer/federation 开启受控 gossip。全部安全 profile 禁用 multicast、shared memory、adminspace 和动态插件加载 |
| 配置覆盖 | 最终配置必须等于 profile 生成的原生配置；禁止额外 route、listener、关闭认证或内嵌凭据 |
| domain | 最终 domain 必须等于 profile 的 `domain_id`；覆盖不能扩权 |

Rust 普通调用使用 `open_secure_context(request, profile)`。已解析所有配置层的适配器使用 `open_resolved_secure_context(resolved, bootstrap, profile)`，后者不会重新读取环境或再次合并 bootstrap。普通 `open_context` 和底层 transport API 仍可用于开发模式；部署入口必须明确选择并保持安全模式。

## Policy 与实际权限

Policy 列出有限的主体、主体拥有的 Node、最终部署后的 endpoint 名称、完整 Interface，以及允许的操作。编译器复用 aos-core 的类型哈希、Action 派生接口、名称和 QoS 校验，不接受手写 Zenoh key expression。namespace 按路径段比较，`/robot` 不包含 `/robot2`。

| 权限 | Router 允许的效果 |
| --- | --- |
| Topic `publish` / `subscribe` | 对指定 domain、名称、类型名和类型哈希发送 / 接收 |
| Service `provide` / `call` | 对指定类型的服务提供响应 / 发起请求 |
| Action `provide` | 提供 send_goal、get_result、cancel_goal 和 feedback、status 五个派生通道 |
| Action 客户端 | send_goal、get_result、cancel_goal、feedback、status 分别授权；能发目标不等于能取消 |
| Parameter | read 与 write 分开；提供服务只能对应主体自己的 Node |
| Lifecycle | read 与 change 分开；提供服务只能对应主体自己的 Node |
| Graph | 只允许读取完整 policy 中已知 Node 的有限声明；不会顺带授予业务数据访问 |

每个本地 Node 自动获得其 Node 声明与两个内置描述服务的提供权限。读取其他 Node 的描述仍需 `describe`。Graph key 只对运行时生成的 ZID、Node ID、entity ID 和 QoS 段使用受限通配符，名称、namespace、endpoint 和类型哈希保持明确。

`Client.wait_for_service` 和 Action 的就绪等待依赖 graph，因此调用主体还需要目标 Node 的 `graph_nodes` 权限。只授予 Service 调用权不会自动公开该 Node 的元数据。QoS 用于展开协议所需权限，graph 的 QoS 段不构成独立的访问控制边界。

Transient-local 会增加当前 Zenoh 扩展所需的缓存 query、reply、存活 token 和可靠传输检测权限。新增 key 仍绑定同一个 domain、Topic 名称、类型与哈希。编译结果保留 `expanded_grants`，每项都标明主体、AOS 操作、资源、方向、原生消息类型和 key expression，可在部署前审阅。

`parameter_events` 是 domain 内固定的全局 Topic。`parameter_events_read` 授权允许读取该 Topic 的事件，`SystemGrant.target` 不能按消息正文中的 Node 字段过滤。`parameters_provide` 除了目标 Node 的参数服务权限，还会授予该全局 Topic 的发布权限，以支持参数服务自动发布事件。

拥有该发布权限的主体可以在 `payload.node` 中填写其他 Node 的名称。该字段可伪造，不能据此认定事件来自所声称的 Node，也不能把事件当作参数状态的权威来源。消费者可把事件当作变更提示；需要确认状态时，应通过已获授权的目标 Node 参数服务读取。Lifecycle 事件使用目标 Node 的 `transition_event` Topic。

编译器限制 64 个主体、256 个 Node、1024 项输入 grant、32768 项展开权限，以及 1 MiB policy JSON。重复主体 ID、重复 CN、重复 Node 所有权、重复操作、越界名称和未知操作都会拒绝。

## 拒绝与观察边界

Zenoh 1.10.1 的原生 publish/declare 调用可能在本地成功，随后被 Router ACL 丢弃；客户端请求也可能只表现为 `Timeout` 或提前结束但没有回复的 `NoResponse`。因此，客户端返回值不能独立证明授权成功或被拒绝。验收同时检查 Router 的实际消息拦截与接收端效果，并用另一身份完成同一路径的授权正例。关键拒绝断言要求同一线程的 ACL `unauthorized` 决策后紧接 `Msg intercepted!`；仅在权限缓存预计算时输出的 `unauthorized` 不算请求被拒绝的证据。

ACL 作用于进入和离开受控 Router、peer 的网络消息。本 Context 能看到自己本地的 graph 实体；进程内部互信、文件系统上的凭据访问以及 CA 签发流程不由网络 ACL 隔离。CN 是部署主体；同一主体下的多个 Node 共享其权限边界。跨 Router 的相邻 CN 是 Router 本身，有限 relay 授权不等于原始调用者身份传递。消息正文中的调用者、goal ID、参数名等内容不会变成额外身份认证。

本阶段不提供多 Router 高可用、证书自动轮换、撤销列表、远程密钥管理、payload 字段授权或任意原生 Zenoh 配置扩展。Router 及其私钥、CA 和 policy 属于受信部署配置。

## 验证与固定源码

`crates/aos-zenoh/tests/security_policy.rs` 检查严格 JSON、文件边界、最终配置覆盖、资源边界、真实 Action 派生哈希、Graph 权限、系统读写拆分与编译规模上限。

`crates/aos-zenoh/tests/security_runtime.rs` 使用临时目录内由 OpenSSL 生成的短期证书和本机 loopback Router。它检查有效 mTLS、错误 CA/SAN、过期证书、缺失证书、密钥不匹配、空信任根、无客户端证书、明文连接，以及 Topic、Service、Parameter、Lifecycle、Action 与 transient-local 的实际通信。Graph 同时检查一次查询和持续 watch。测试凭据不提交到仓库，测试结束后删除临时目录。

其中，空显式 CA 的客户端连接私有 CA Router 失败，只能称为 fail-closed 观察。即使客户端保留公共根，私有 CA 的 Router 也可能被拒绝，因此这项握手结果不能证明没有公共根回退。

根证书选择由 `vendor/zenoh-link-tls/src/utils.rs` 中的构造路径测试另行验证。三个测试实际调用 `TlsClientConfig::new`，在同一 `RootCertStore` 交给 rustls 前观察其根数量：显式一个 CA 为 1，显式空 CA 为 0，未配置 CA 则等于上游公共根集合的数量。观察字段仅在 `#[cfg(test)]` 下存在。测试使用内嵌的公开测试证书，不保留其私钥，也不连接外部服务。将构造函数恢复为固定上游的“公共根加显式 CA”逻辑时，同一组显式 CA 断言必须失败；只测试独立 helper 无法覆盖这条接线。

这里的 Router 是当前 workspace 依赖构建的内嵌 `zenoh::Session`。测试不证明外部 `zenohd` 二进制的构建特性、配置加载或安装部署。私有根补丁只改客户端行为；上游同版本的服务端 mTLS 已使用显式 CA 验证客户端。可安装 Router 入口的验收仍需对其实际进程和 TLS listener 单独验证。

显式私有根和 TLS 并发读写修复位于独立 vendored `zenoh-link-tls 1.10.1`，上游 commit 为 `1211779c3647f5a96713dade452c546a07823580`，原始 crate SHA256 为 `cce65fd71b4631821a85058542e28faac0151af6c53cd61f2dfafc7e16c4d6c4`。只修改 `src/utils.rs`、`src/unicast.rs` 和 `src/lib.rs`，保留全部原始源码，附固定上游 LICENSE、NOTICE 和修改说明。两个独立构建常量会使遗漏私有根补丁或 I/O 补丁的构建失败。

固定上游的 TLS 实现通过 `UnsafeCell` 和独立读写锁同时取得同一个 `TlsStream` 的可变引用；读写都会修改 rustls 连接状态，因此存在数据竞争。AOS 改用 `tokio::io::split`，在每次 poll 内串行访问 TLS 状态，并保留每个方向的完整操作顺序。每次写入在同一操作锁内 flush，避免最后一段密文等待下一次写入才送出。显式关闭发送 TLS close_notify；释放连接不再进入另一个 runtime。三个真实 TLS 回归检查双向并发数据完整性、挂起读取期间的写入与关闭，以及 runtime 上下文外的 socket 释放；数据完整性断言必须在关闭连接前完成。

运行源码验证：

```sh
python scripts/check_zenoh_tls_vendor.py --archive /absolute/path/zenoh-link-tls-1.10.1.crate --json
python -m unittest discover -s scripts -p test_zenoh_tls_vendor.py -v
cargo test -p zenoh-link-tls --lib aos_private_roots_tests
cargo test -p zenoh-link-tls --lib duplex_tests
cargo test -p aos-zenoh --test security_policy
cargo test -p aos-zenoh --test security_runtime -- --nocapture
```

验证器不下载源码。它核对原始 archive、固定文件变更集合、license 哈希和完整 patch replay；测试另外检查修改 manifest 后仍不能隐藏源码、license 或补丁越界。源码校验通过仅证明来源和补丁可重放，运行时验收以实际测试结果为准。
