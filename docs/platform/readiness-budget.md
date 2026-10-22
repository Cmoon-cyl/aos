# Readiness 的发现预算与生命周期

`wait_for_service(timeout)` 和 `wait_for_server(timeout)` 在一次调用内保留完整的发现过程。远端服务的发现响应超过 50ms 时，不应因为本地关闭检测的轮询间隔而反复丢弃响应。`None` 没有应用层截止时间；超时返回 `false`，client、node 或 Context 关闭返回 `Error::Closed`。

Zenoh 适配器使用一个带 `history(true)` 的 liveliness subscriber，直到本次等待结束才释放。50ms 仅用于检查 client/node 的关闭状态，Context shutdown 通过注册的通知队列直接唤醒等待。发现过程只读取图实体，不发送业务 Service 请求，也不提交 Action goal。

## API 与职责

| 层 | 职责 |
| --- | --- |
| `Transport::watch_graph()` | 返回持有网络发现 interest 的 watch。默认 `None` 仅保留旧自定义 backend 的兼容路径。Zenoh 正式实现始终返回 `Some`。 |
| `GraphWatchBackend::snapshot()` | 返回当前已在本地发现且未被移除的实体集合；不是全网同步屏障。 |
| `GraphWatchBackend::wait_for_change(timeout)` | 只等待本地通知，允许合并通知。调用方醒来后必须重新取 snapshot，不能把通知数量当作实体数量。 |
| `Context::watch_graph()` | 公共返回类型不变；私有包装持有 Context clone，并在 snapshot/wait 前后检查 Context。最后一个 watch 被释放后，才允许其独占持有的 Context 关闭。 |
| `Common::wait_for_ready()` | 按 server kind 和完整 endpoint name/type/hash 匹配；负责调用期限与 client/node 关闭检查。 |
| typed Publisher | 继续使用原有 matching listener，完整等待调用方给出的预算。 |

旧自定义 backend 返回 `None` 时仍使用原先的 snapshot polling。这项兼容路径没有获得 Zenoh 的持续 interest 语义。应用可以直接复用一个 Context watch，在同一等待预算内检查订阅者数量等条件。

每次 SDK readiness 调用拥有自己的 watch。调用结束就释放；多个短调用的预算不会相加。需要等待跨机冷启动发现时，应在一次调用中提供完整期限，或使用 `None` 并通过关闭取消。短调用是否能命中已有缓存取决于当时已经观察到的图状态，不能据此承诺节点级的永久 graph cache。

## 固定源码依据

Cargo 中的 Zenoh 1.10.1 对应上游提交 `1211779c3647f5a96713dade452c546a07823580`，由该包的 `.cargo_vcs_info.json` 核实：

- `zenoh/src/api/builders/liveliness.rs` 的 `history(true)` 和 subscriber 构造设置历史回放，并启用 undeclare-on-drop。
- `zenoh/src/api/session.rs::declare_liveliness_subscriber_inner` 发送 `InterestMode::CurrentFuture`，没有把本地等待间隔设置成网络 GET timeout。
- `zenoh/src/api/subscriber.rs::Drop` 调用 `undeclare_subscriber_inner`；liveliness 分支发送 `InterestMode::Final`。
- `session.rs` 的 `UndeclareToken` 分支删除远端 token 并发出 Delete sample。AOS watch 将它从当前集合移除。

发布版 1.10.1 的本地历史 Put 回放与后续 live Delete 并发，可能让一个持续 watch 在处理 Delete 后又收到旧 Put。AOS 使用固定版本的 vendored history gate，让该 watch 先完成历史回放，再按顺序交付期间到达的 live 事件；routing outbox 进一步保证同一目标上路由生成的 Current Declare 与后续撤销按生成顺序发送，避免接收方缓存被旧声明重新写入。补丁不改变调用预算，也不引入永久墓碑；同 key 断线恢复后的 Put 仍可交付。来源、异常清理语义和独立回归见 [`upstream/zenoh-history-gate.md`](../../upstream/zenoh-history-gate.md) 与 [`upstream/zenoh-routing-order.md`](../../upstream/zenoh-routing-order.md)。

另一个 session 观察到 provider 消失，不代表调用方已经收到同一移除。因此，刚重建的 readiness watch 仍可能从本地已知状态返回 `true`。测试必须在调用方持续 watch 上等待后续网络标记及移除收敛，不能把跨 session 的观察当作即时 `false` 屏障。

生产 Node 和 endpoint 的 ID 均由 `Uuid::new_v4().simple()` 生成，分别位于 `Node::new_resolved` 和 `Node::common`。watch 保留旧 snapshot 按 entity ID 去重的行为；直接实现 transport 的自定义声明者仍须遵守 ID 唯一的接口约定。

## 回归范围

`crates/aos-zenoh/tests/readiness.rs` 使用真实 TCP 代理给两个方向分别加入至少 120ms 转发延迟，并关闭延迟观察端的其他连接路径。前置的三个 50ms GET 连续漏掉服务；真正测量 readiness 耗时时另建观察端，防止前置 GET 的迟到声明预热缓存。

测试覆盖：

- Service 和 Action 的晚启动 provider、无限等待、有限截止时间、完整 endpoint 身份检查。
- Service/Action client、Node、Context 关闭；typed Publisher 的完整 matching 等待预算；全程没有业务请求或 goal admission。
- 已有 token、后续 token、移除、多订阅者数量变化，以及同 session 的 Service/Action readiness。
- watch A 释放后 provider 消失，由另一 session 先确认移除；记录调用方首次 readiness 结果，再保留 watch B，等待 live marker 和本地移除收敛，最后重建 watch C 并用新 marker 验证。首次结果允许短暂 `true`，B/C 不得持续保留已移除实体。
- 已有本地 history 时，连续八轮并发创建新 watch 和移除 provider；后续 marker 到达后检查当前集合。
- router 尚未连接时创建 watch，随后连接 router 并读取该 router 自己持有的当前 token。这个测试隔离验证 interest 的晚连接回放，不代表任意自定义 peer 拓扑的跨 peer 发现都已验收。
- watch 单独保活 Context、最后 Drop 使 router 观察到连接消失，以及 Context 显式 shutdown 唤醒 60 秒等待。
- 独立单测检查声明失败释放注册队列，正常 Drop 释放 subscriber callback 与通知队列。

这些回归测试覆盖本机 TCP 行为；跨机部署仍需检查实际网络中的发现与业务就绪。
