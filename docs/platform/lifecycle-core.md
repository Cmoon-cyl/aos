# Lifecycle 与参数修改状态限制

LifecycleNode 显式启用受管行为，普通 Node 继续使用原有行为。每个受管 Node 只有一个 Rust `Lifecycle` 状态所有者，参数存储、业务实体与控制服务读取同一状态。SDK 负责调用应用回调，CLI 负责请求和展示；两者都不维护第二套状态机。

## 来源与职责

状态图采用固定 Jazzy rcl [`18d96c9c` 的 `default_state_machine.c`](https://github.com/ros2/rcl/blob/18d96c9c2b98e616324ae9a2c7701dd0b0886ff0/rcl_lifecycle/src/default_state_machine.c)，共 25 条边。回调返回值与 error 回调阶段参考固定 rclpy [`4e607c09` 的 `lifecycle/node.py`](https://github.com/ros2/rclpy/blob/4e607c09d624debc39a4ae5405680c06d87fac35/rclpy/rclpy/lifecycle/node.py)。跨实体业务准入、在途租约、排空报告及参数 `allowed_states` 是 AOS 增加的契约，不能从 ROS 状态名称推断这些行为自动成立。

| 层 | 负责 | 不负责 |
| --- | --- | --- |
| Rust core | 状态图、转换串行化、业务准入、在途租约、排空、参数状态限制、服务 Schema | 执行应用回调、终止用户代码、替 Action 合成终态 |
| Zenoh adapter | 有界队列、真实请求/回复、`Inactive` wire 错误、请求 ID | 判断业务生命周期、执行 Python 回调 |
| SDK / Executor | 在应用执行器调用转换与业务回调；在回调完成后释放 core 所有的工作对象 | 复制状态机、在 transport 回调执行应用代码、把超时当成工作已停止 |
| CLI | 调用控制服务，展示状态、报告及 remaining | 通过本地推测修改远端状态或 Goal 状态 |

## 控制与转换

Rust 使用 `Node::new_lifecycle`、`new_lifecycle_with_config` 或 `new_resolved_lifecycle` 创建受管 Node。`node.lifecycle()` 返回同一状态所有者。Node 自动发布 `~/transition_event`；调用 `enable_lifecycle_services()` 安装五个控制服务：

- `~/change_state`
- `~/get_state`
- `~/get_available_states`
- `~/get_available_transitions`
- `~/get_transition_graph`

查询服务由原生 worker 响应。`change_state` 由 Executor 取得请求、调用 `begin_transition()`、执行对应回调，再将回调结果交给转换 token。若 core 返回 `errorprocessing`，Executor 继续调用 error 回调并完成同一 token。回调执行期间不持有生命周期状态锁，参数、状态查询、Action result/cancel 等控制操作可以继续执行。单线程执行器无法抢占一个尚未返回的同步回调。

`get_state` 在一次加锁中读取当前状态、业务门禁、epoch、在途工作与最近转换报告。初始为 `unconfigured`。主状态为 `unconfigured/inactive/active/finalized`；查询返回的完整状态集合另含 `unknown` 和六个转换状态。外部只能请求公开转换，回调完成边由 token 推进。

默认排空等待为 5 秒，使用 steady 时间；该期限在转换回调返回之后计算，不是回调执行超时。deactivate 排空超时遵循 failure 边回到 `active`，报告真实剩余工作；shutdown 的 failure 边进入 `finalized`，也保留剩余工作。原回调返回值与 `drain_timed_out` 分开记录。没有返回的回调保持转换中，不伪报结束。

`SingleThreadedExecutor` 在执行转换的同一 spin 线程上等待排空。若仍有持有租约、需要这个线程执行或继续执行的业务回调，等待期间它们无法推进，排空会超时；已经在其他线程运行并可独立结束的工作仍可能及时释放租约。需要先完成这些回调再转换，或使用能够继续调度它们的执行器配置，不能把延长 drain 时限当作解除同线程等待的方法。AOS 的排空等待是新增行为，不是从 ROS lifecycle 状态图自动得到的保证。

Python 本地 `drain_timeout` 若无法表示为 steady 截止时间，在取得转换 token 或调用应用回调之前返回参数错误，状态保持原样。`change_state` 请求通过 JSON Schema 后若仍无法解析为 Rust 请求类型，会回复结构化 `VALIDATION` 错误；该失败不启动转换，也不会把服务从 Executor 中移除。

解析失败后的错误回复如果也因传输错误失败，Executor 仍收到原来的 `Validation`，不因第二次错误退出。远端仍可能因未收到回复而超时；这不代表转换曾经启动。

未完成的转换 token 被丢弃时，core 通过错误边进入 `finalized`，报告 `abandoned=true`，不声称 error 回调已经执行。进入 `finalized` 不等于销毁 Node；直到显式销毁 Node 或关闭 Context，控制服务仍可用。

## 受管业务

业务只有在 `active` 时才能取得新租约。门禁检查和受理在 Rust 中完成；已取得租约的工作继续使用该租约，不会因 epoch 改变而被假定取消。

回调租约只覆盖该次 Timer 或 Subscription 回调；回调内新发起的 `publish()` 仍需自己的准入。
因此，并发 deactivate 可能发生在回调开始之后、发布之前，使该次发布返回 `InactiveError`。
周期状态发布可精确捕获这个错误，记录一次跳过后结束回调，不自动重试。不要捕获所有异常或只在
发布前查询状态：查询与发布之间仍可能发生转换。未处理的应用异常会由 Executor 抛出，应用必须
观察 spin 线程的异常；线程已退出不等于执行器正常完成。

执行 `on_activate` 时状态仍为 `activating`，受管 Publisher 此时发布会返回 `Inactive`。只有激活回调成功返回并提交到 `active` 后，普通受管发布才可进行。应用应把首次业务发布安排在激活完成后；不能照搬在 ROS 激活回调内部先激活 publisher 再发布的写法，AOS 没有独立的 publisher 激活开关。

| 实体 | 新工作准入 | 已受理工作与排空 |
| --- | --- | --- |
| Publisher | 非 active 返回 `Inactive` | 编码和提交期间持有短租约 |
| Subscription | 使用 `take_message()` 取得带租约的 `SubscriptionCall`；受管 `recv_timeout()` / `recv_sample_timeout()` 拒绝绕过租约 | 已交给 Executor 的工作直到回调完成或丢弃才释放租约；`sample()` 保留同一消息的原始接收/来源元数据 |
| Timer / Rate | Timer 非 active 不生成就绪调用；Rate 返回 `Inactive` | `TimerCall` 包括排队等待阶段都持有租约；已受理调用可完成 |
| Service provider | 非 active 原生 worker 明确回复 `Inactive`，无需业务 Executor 消费拒绝请求 | `ServiceRequest` 回复或释放时结束租约；超时不伪造回复 |
| ActionServer | 非 active 原生拒绝新 Goal；admission 回调后再次核对原 epoch | 接受 Goal 后租约延续到执行结果完成；仅进入成功/取消/失败终态不会提前释放 |
| ActionClient | 发送新 Goal 需要 active；feedback 回调使用 Subscription 工作租约 | 查询 result/cancel/status 保持可用，客户端不推进 provider 状态 |

Subscription 停用期间由原生 worker 丢弃消息，并提供独立 `lifecycle_skipped_total`；transport 的 QoS 队列统计保持自己的含义。激活准备清理本地剩余队列、重置 Timer 周期，避免补派已停用积压。该边界是本地接收边界，不能仅凭远端发送时间证明一条尚在网络中的消息属于上一次 active 周期。

`SubscriptionCall.message()` 继续返回业务 JSON；`sample()` 额外提供 adapter 原始序列化字节数、接收时间和可用的来源证据，时间不会在 Executor 出队时重建。旧 adapter 缺失元数据时保持空值，不能用本地出队时间补成接收时间。

Action 的 status/get_result/cancel 及 provider 为已接受 Goal 发送 feedback，是现有 Goal 的控制和完成路径。在 provider 停用或 shutdown 排空期间可以继续；core 不合成 `CANCELED`，也不强行关闭这些通道。接收 feedback 的 Node 按自己的生命周期接纳业务回调：Rust 受管客户端使用 `take_feedback()` 并保留返回的 `SubscriptionCall`，直到处理结束再 `finish()` 或释放；返回裸值的 `recv_feedback()` 明确返回 `Unsupported`，不能提前释放排空租约。仅返回业务终态而没有完成结果的 Goal 仍在排空报告内。

## 参数 `allowed_states`

`ParameterDescriptor.allowed_states` 是字符串数组，允许的值为 `unconfigured`、`inactive`、`active`、`finalized`；不接受 unknown、过渡状态或重复值。空数组表示不限制，因此普通 Node 和未设置限制的控制参数保留原行为。

非空限制要求参数属于 LifecycleNode；普通 Node 声明这类 descriptor 返回 `unavailable`。声明初值和启动 override 仍可在任意状态安装，但类型、范围和应用校验仍生效。`allowed_states` 约束后续 set、动态参数删除及 load；load 使用目标 Node 已声明的 descriptor，不能用文件中的 descriptor 放宽限制。

prepare 在不允许的状态返回 `state_not_allowed`，不运行应用 validator。允许时记录同一 Lifecycle 的 epoch，validator 在锁外执行；commit 重新核对 epoch。期间发生转换，即使后来回到同名状态，也返回 `conflict`。应用需要重新读取状态并发起新的事务。

受限提交与生命周期状态变化共用短暂的提交屏障，保证状态校验和参数写入之间不会插入转换。屏障不占用业务准入锁，避免与 Timer/Clock 的锁顺序反转；`use_sim_time` 和受限参数同一批提交也遵循这一约束。网络参数事件在释放状态锁和屏障之后发布。

批量失败保持所有参数值和版本不变，不发布成功事件。descriptor 可通过 describe 查询和 dump 保存；新增字段改变 describe 服务的 Schema/hash，通信双方需使用相同接口版本。旧离线 descriptor 缺少该字段时按空数组解析。

快照加载对 `NotSet`、只读同值及无变更批次的处理见[参数快照加载](parameter-load.md)。这些项仍参加状态限制和应用校验。

当前 Zenoh adapter 的 Service/Subscription 接收队列只返回数据、`Timeout` 或 `Closed`；接收 worker 对 `Closed` 结束，对超时继续，查询和 Action worker 还会继续处理校验错误。抽象 Transport 若返回其他接收错误，worker 会结束；当前没有把网络瞬断证明为该路径的故障，也未增加无限重试。

本地 core 测试覆盖状态图、失败/error/放弃、每类实体准入、排空超时、跨周期积压、Action 完成路径、参数本地与远程服务限制、拒绝无事件，以及受限时钟批量更新与 Timer 并发。安装后的 SDK、真实 transport 与跨机验收由相应集成测试单独证明。
