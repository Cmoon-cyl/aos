# AOS bag 共用运行层

本页定义 0.2.0 的 bag 运行契约，Rust 运行层已接入 CLI/Python；当前平台结果见[发行说明](../releases/v0.2.0.md)，历史跨机记录保留各自固定来源。实现位于 [`aos-bag`](https://github.com/Cmoon-cyl/aos/blob/v0.2.0/crates/aos-bag/README.md)，依赖 Stage3/Stage4 和 core raw/ActionObservation 接口。本文补全[已冻结格式](bag-format.md)的运行接口；操作入口见 [bag-cli.md](bag-cli.md)。CLI 与 Python 只适配 Rust 运行层；不另写录制、排序或时间控制算法。

## 分层与 core 接口

`aos-bag` crate 依赖 `aos-core`。core 只补原始消息收发与 Action 观察入口；MCAP、文件、录制队列、恢复和回放调度均归 `aos-bag`。

| 层 | 必须负责 | 不负责 |
|---|---|---|
| `aos-core` | 原始字节收发时仍执行 Interface 校验、Node 关闭、lifecycle、名称解析；生成并核对 Action feedback/status 类型 | MCAP、录制目录、暂停/seek、文件恢复 |
| `aos-bag` | 动态发现、Interface 保存、MCAP、容量/分片、统计、恢复、回放状态和 steady 调度 | Zenoh key 构造、调用 Service、提交/取消 goal、设置参数 |
| Zenoh adapter | 现有 typed endpoint、QoS、ACL、收到的 Sample 元数据与队列统计 | 将 bag 数据解释成业务操作 |
| CLI / PyO3 | 配置解析、Context 创建、命令/信号转发、序列化返回值；Python 等待时释放 GIL | 另一套 recorder/player、改写 payload、另算 ROS 时间 |

core 提供：

```rust
impl TypedSubscription {
    pub fn recv_serialized_timeout(
        &self,
        timeout: Duration,
    ) -> aos_core::Result<Sample<Vec<u8>>>;
}

impl TypedPublisher {
    pub fn publish_serialized(&self, bytes: &[u8]) -> aos_core::Result<()>;
}

pub enum ActionObservationKind {
    Feedback,
    Status,
}

impl Node {
    pub fn create_action_observation_subscription(
        &self,
        action: &Interface,
        action_name: &str,
        kind: ActionObservationKind,
        qos: impl Into<ResolvedQoS>,
        options: EndpointOptions,
    ) -> aos_core::Result<TypedSubscription>;

    pub fn create_action_observation_publisher(
        &self,
        action: &Interface,
        action_name: &str,
        kind: ActionObservationKind,
        qos: impl Into<ResolvedQoS>,
        options: EndpointOptions,
    ) -> aos_core::Result<TypedPublisher>;
}
```

`recv_serialized_timeout` 解码并校验，但返回同一份原始字节和 Sample 元数据；不得用 `serde_json::to_vec` 重建 payload。与现有 `recv_sample_timeout` 一致，managed subscription 仍要求 callback lease，不能通过这个方法绕过 lifecycle；bag 自己创建普通 Node。`publish_serialized` 先取得现有 publisher lifecycle lease，再校验并把原字节交给 backend。

Action 入口只接受完整 `kind=action` Interface、Action base 名称和上述两种观察类型。先按 `NameKind::Action` 解析 base，再生成固定后缀；不能传入任意内部 endpoint 或任意 message Interface。返回值只有 Topic 能力，不创建 ActionClient/ActionServer，也不持有 goal、result、cancel client。

基线事实：当前 `config.rs` 拒绝分别 remap `/_action/*`，但 `Node::create_publisher/subscription` 本身没有禁止直接创建这些名称，ActionClient/Server 也复用普通方法。本文新增受限入口并让 bag 只走这个入口；是否同时收口现有公共构造方法，应由 core 所有者单独处理，不能在 bag 中假装该限制已存在。

## 固定依赖和复用依据

workspace 已固定：

```toml
mcap = { version = "=0.26.0", default-features = false }
aos-bag = { path = "crates/aos-bag" }
```

crate 的其他直接依赖使用现有 `aos-core`、`serde`、`serde_json`、`sha2`、`uuid`；测试使用现有 `aos-zenoh`。先不开启 LZ4、Zstd 或 Tokio，也不新增 mmap 依赖。

| 项目 | 已核对事实 |
|---|---|
| 发布包 | `mcap 0.26.0`；Cargo 索引发布时间 `2026-10-19T20:43:24Z`；未 yank；MSRV `1.81` |
| crate SHA256 | `25b1b2f270daee3b73d6b723a62a3121f228387114fbd831e3727542927232b6` |
| 发布源码 | `.cargo_vcs_info.json` 指向 `968de1453db8a5a39c538dfac5a2a81455dd050f` 的 `rust/mcap` |
| 许可 | Cargo 声明 MIT，已读取同一源码提交的 [LICENSE](https://github.com/foxglove/mcap/blob/968de1453db8a5a39c538dfac5a2a81455dd050f/LICENSE) |
| 写入 | 上游 [`Writer`](https://github.com/foxglove/mcap/blob/968de1453db8a5a39c538dfac5a2a81455dd050f/rust/mcap/src/write.rs)：`add_schema`、`add_channel`、`write_to_known_channel`、`flush`、`finish` |
| 读取 | 上游 [`io::MessageReader`](https://github.com/foxglove/mcap/blob/968de1453db8a5a39c538dfac5a2a81455dd050f/rust/mcap/src/io.rs) 和 [`sans_io::LinearReader`](https://github.com/foxglove/mcap/blob/968de1453db8a5a39c538dfac5a2a81455dd050f/rust/mcap/src/sans_io/linear_reader.rs)：流式读取、record 长度上限、chunk CRC 预校验 |
| ROS 参考 | rosbag2 Jazzy `12b5e8ba4eb5665501a16f0d9e050552a1149f56`；独立实现 Rust 行为，不引入 ROS 运行时 |

rosbag2 [`recorder.cpp`](https://github.com/ros2/rosbag2/blob/12b5e8ba4eb5665501a16f0d9e050552a1149f56/rosbag2_transport/src/rosbag2_transport/recorder.cpp) 先注册 topic metadata，再创建 GenericSubscription，并传递 serialized message；AOS 采用同样的先后约束。其 [`time_controller_clock.cpp`](https://github.com/ros2/rosbag2/blob/12b5e8ba4eb5665501a16f0d9e050552a1149f56/rosbag2_cpp/src/rosbag2_cpp/clocks/time_controller_clock.cpp) 在暂停、恢复、变速和跳转时重置 steady/播放时间锚点；[`player.cpp`](https://github.com/ros2/rosbag2/blob/12b5e8ba4eb5665501a16f0d9e050552a1149f56/rosbag2_transport/src/rosbag2_transport/player.cpp) 的 seek 停止读取、清空旧队列、移动 reader 和 clock，再继续读取。AOS 固定为包含目标时刻的 seek，且不采用该 player 的 Service 请求回放能力。

源码接口说明不等于 AOS 功能验证。特别是 MCAP reader 默认不检查 chunk CRC；AOS 必须显式启用检查。

## 最小公共 API

下列名称用于固定 SDK/CLI 接入边界。配置与结果结构使用严格 serde DTO；未知字段、非有限 rate、负数、溢出和互斥选项在创建网络/文件实体前返回错误。

```rust
pub fn inspect(path: &Path, options: InspectOptions) -> BagResult<BagInfo>;
pub fn recover(
    source: &Path,
    destination: &Path,
    options: RecoveryOptions,
) -> BagResult<RecoveryReport>;

impl Recorder {
    pub fn start(context: Context, options: RecordOptions) -> BagResult<Self>;
    pub fn status(&self) -> RecordStatus;
    pub fn stop(&self, reason: RecordStopReason) -> BagResult<()>;
    pub fn wait(&self, timeout: Duration) -> BagResult<RecordReport>;
}

impl Player {
    pub fn open(
        context: Context,
        source: &Path,
        options: PlaybackOptions,
    ) -> BagResult<Self>;
    pub fn start(&self) -> BagResult<PlaybackStatus>;
    pub fn pause(&self) -> BagResult<PlaybackStatus>;
    pub fn resume(&self) -> BagResult<PlaybackStatus>;
    pub fn seek(&self, offset_ns: u64) -> BagResult<PlaybackStatus>;
    pub fn set_rate(&self, rate: f64) -> BagResult<PlaybackStatus>;
    pub fn stop(&self) -> BagResult<()>;
    pub fn status(&self) -> PlaybackStatus;
    pub fn progress(&self) -> PlaybackProgress;
    pub fn wait(&self, timeout: Duration) -> BagResult<PlaybackReport>;
}
```

`stop` 提交停止请求；`wait` 返回 worker 结束后的最终报告。只有报告的 `resources_closed=true` 且 `cleanup_error=null` 才确认自身实体和文件已关闭；运行错误与清理错误分别保留。超时不等于已经停止。Player 控制使用可配置的 steady 截止时间；成功只确认本地 cursor、timeline 和 epoch 已提交，控制与取消竞争时由同一把锁确定结果。clock 在确认之后发送，发送失败随后体现为 `failed/error`。Drop 尽力发起清理，显式 `stop` + `wait` 是可核对结果的路径。

Player 的 `progress()` 返回状态、时间、计数和 endpoint 数量，适用于持续轮询。完整 `status()` 和控制回复仍包含 endpoints，但在 dispatch 状态锁外复制这些静态描述。外部先关闭 Context 时，实体的 `Closed` 清理结果表示已经关闭，不另记 cleanup error；其他清理失败保留在 `cleanup_error`，没有更早运行错误时也成为 `error` 并使状态进入 Failed。

Manifest 的可选 `cleanup_error` 保存分片 finish、flush、hash、rename 或 manifest 收尾错误。分片收尾和最终 manifest 写入同时失败时，`cleanup_error.message` 按发生顺序保留两项错误，`kind` 和 `file_offset` 保留首项的值。已有主 `error` 的 kind、内容和 offset 保留；没有主错误时，首个收尾错误同时成为主错误。最终写入失败后的最新诊断保留在内存中，磁盘 manifest 可能仍是旧状态。`resources_closed` 表示所有清理步骤已确认成功，false 不等于 fd 泄漏：发生分片收尾错误时，文件句柄仍通过 Drop 释放，但文件完整性或持久化步骤未成功。

| DTO | 关键字段 |
|---|---|
| `RecordOptions` | 新目录、Node 配置、显式 Topic 列表或 `all_topics`、排除项、默认/逐 Topic QoS、发现周期/超时、队列条数/字节上限、消息/Schema/channel 上限、chunk 刷新条件、分片条件、总容量 |
| `PlaybackOptions` | Node 配置、目标 namespace 或显式原名模式、选择项、core remapping、QoS 选择/覆盖、初始 rate、初始暂停、可选 clock、读取/控制资源上限 |
| `RecordStatus` | 状态、发现/已订阅/未解析端点、已接收/已写入消息及字节、adapter drop、录制队列 drop、过大消息、校验失败/资源拒收、分片、停止原因 |
| `PlaybackStatus` | ready/stopped/paused/playing/finished/failed、recording 偏移、rate、调度 epoch、已发布计数、目标名称与有效 QoS、clock 配置、失败原因 |
| `BagInfo` | profile/version、recording id、每片 SHA256/范围/完整性、完整 Channel/Interface/QoS/时间来源、消息数、丢样来源与未知值、结束原因 |

录制器和播放器创建自己拥有的普通 Node；其关闭不 shutdown 调用方提供的 Context。网络配置和认证继续由调用方创建 Context 时决定。

Bag 的 Node 配置按普通 Node 的规则先合并 Context 保存的 `process_bootstrap`，再解析名称、namespace、remap 和有效配置。同一来源的 API/launcher 字段冲突会失败，不能重复合并或静默覆盖。Player 的数据与 clock 目标预检使用这份合并后的配置，随后交给 `Node::new_resolved`；启动配置中的 remap 也必须满足回放 namespace 边界。Recorder 同样应用启动配置，但选中来源的 remap 仍不能改写订阅目标，否则会让记录标签与实际来源不符，发现时返回错误。

## 动态录制和原始字节

发现使用 `Context::watch_graph`，不支持 watch 的自定义 backend 才按有限周期调用 snapshot。只从选中的 Publisher 获取数据；自己的 recorder/player 节点不作为新录制目标。ActionServer 仅用于解析观察类型，不调用其业务方法。

每个 `(展开名称, type_name, type_hash)` 独立解析完整 Interface。远端描述返回后重新核对名称/hash；先持久化该 channel 所需的 Schema 和 Interface，再允许消息进入写队列。描述暂不可得的端点保持 `unresolved` 并重试，状态中显示原因，不使用空 Schema 或猜测 payload。

core 对本地文件、远端描述和各语言绑定统一使用不回溯的 `regex` 引擎。
`pattern` 与 `patternProperties` 不支持环视、反向引用；每个正则的编译大小上限为
1 MiB，DFA 缓存上限为 256 KiB。超出范围的 Interface 在加载时拒绝。
这是平台支持的 JSON Schema 正则范围；不会把不支持的表达式当作校验成功。
这两个限制按正则计算，不代表整个 Interface 或进程的内存上限。

远端类型描述使用 `get_type_description_bounded`：完整回复最多 4 MiB，
观察消息的 Schema 在编译前受 `min(storage.limits.schema_bytes, 1 MiB)` 限制。
Zenoh adapter 在复制回复前检查字节数；兼容旧接口的自定义 adapter 只能在
`call` 返回 `Vec` 后检查，core 仍会在 JSON 解码前复核。Action parent 使用
共同的 Interface 限制，录包的较小 Schema 额度只用于最终观察消息。

NodeCard 使用 `get_node_card_bounded`，原始回复默认最多 1 MiB；
`get_node_card` 同样应用这个上限，调用方只能将额度收紧。字节检查与类型描述
共用 adapter 前置检查、旧 adapter 返回 Vec 后检查及 core 解码前复核。
超限 provider 保持 unresolved 并退避，已经订阅的健康 Topic 继续录制。

所有 Interface 还遵守[共同编译与校验资源契约](resources.md)：
限制结构、引用展开和正则总量，并在校验前按 Schema 与 payload 计算工作量，
超过 8,388,608 单位时拒绝该消息。
这包括语法和业务 Schema 都合法、但超出资源预算的 payload；Recorder 将它计入
`resource_rejected_total`，继续收取其他消息。JSON 或业务 Schema 不匹配仍计入
`validation_failed_total`。这两种拒收都不使整个 Recorder 停止。该预算是确定的准入规则，
不承诺某个 CPU 时限或整个进程的 RSS 上限。

接收 worker 串行地每轮从每个订阅收取一条；工作量准入没有提供每端点速率或
CPU 配额。R3 release 探针对一个边界 payload 曾测到单条约 40.6 ms，
这是此前版本的单次观测，不是本轮性能验收或延迟上界。若相同机器与负载下
每条仍需约 40 ms，N 个持续发送此类消息的 Topic 会给每轮增加约 N×40 ms
校验时间；这是估算，不是普适下界。健康 Topic 仍每轮只被服务一次，
高频消息可能在有限 adapter 队列中丢弃。现有低速交错回归只证明拒收后能够继续，
没有证明持续洪泛下的公平性或 100 Hz 健康流无损。

每轮发现共享 `description_timeout_ms` 预算。剩余时间不足其四分之一时，
尚未开始的 node card、类型或 Action parent 描述请求推迟到后续轮次，
不增加描述失败计数，也不进入失败退避。已经发出的 RPC 仍受剩余时间限制；
它的失败不会因另一个 provider 尚未开始而被改写为推迟。失败后保留下一位
provider 的位置，并让后续端点获得下一轮预算，避免一个挂起的描述服务挡住全部新 Topic。
Action 观察类型和 parent 类型可以跨两轮取得；最多保留一份已校验的观察
Interface，下一轮优先继续其 parent 请求。parent 失败会清除这份进度并正常退避。
续传槽已被其他端点占用时，新端点同样推迟；保留原续传，不计描述失败或进入失败退避。
发现的等待只由停止信号或下一轮期限唤醒；样本入队不唤醒发现线程。
每个订阅的停止检查使用原子标志，不在空闲扫描中逐项获取全局状态锁。

描述登记在 writer 尚未开始时可以取消；已经开始的写盘允许收尾。
若完成结果在超时判断前已可见，调用返回该结果；否则返回 Timeout，
端点保持 unresolved。超时计入 `descriptor_registration_timeouts_total`；
超时后成功收尾计入 `descriptor_registration_late_commits_total`。
后者可能暂未对应活动订阅，但会占用描述额度；同一 channel id 重试复用既有登记，
不重复占用额度。停止后可从最终报告核对这两项，不能把 Timeout 理解为从未写盘。

同名不同 hash 建立不同 subscription/channel。offered QoS 发现快照、实际 recorder QoS、时间来源或 sequence epoch 改变时建立新 channel。旧 channel 的 metadata 不覆写。发现快照只描述观察到的 publishers，不能用来宣称某条消息来自某位 publisher。

普通 Topic、参数事件和 lifecycle 事件走同一 Message 路径。Action feedback/status 还保存完整 parent Action Interface；离线打开时由 `ActionInterfaces::new(parent)` 重建对应 message Interface，并核对 stored derived hash。即使 provider 已离线，也能解释和回放。

Service/Action 诊断观察 Topic 按普通 Message 录制。[请求诊断适配器](request-observer.md)由 provider/consumer 在自身进程内显式捕获真实 Core records，通过独立 Context 发布固定 `RequestTelemetryV1` Message；安装后的 `request_observer` 使用同一解析路径消费实时和隔离回放 Topic。没有启用、没有发布或实际丢失的数据仍标为不可得。录制器不会为补诊断而执行 Service、send_goal、get_result 或 cancel_goal。该接线候选仍需真实 Core→Topic→MCAP、跨机生产者离线回放和在线业务端点零调用验收。

接收 worker 只做收取、校验、有限复制和入队；文件 I/O 在 writer worker。writer queue 同时限制消息数和 payload/metadata 字节，采用确定的 drop-newest；所有 worker 共享同一计数器。单个过大消息单独计数，不用移除多个正常消息为其腾出空间。

Recorder 的每个普通 Topic、Action feedback/status 订阅都向 adapter 显式传递
`message_bytes` 与 `adapter_queue_bytes`。后者限制单个 adapter 队列中 payload 与
复制预留之和；多个端点分别持有预算，不能把它当作整个 Recorder 的内存上限。
`depth * message_bytes <= adapter_queue_bytes` 仍是声明前的配置检查。
不支持显式预算的自定义 adapter 返回 Unsupported，端点保持 unresolved，不静默降级。

adapter queue drop、writer queue drop、校验失败、校验资源拒收和源 sequence 观测分别报告。
`received_total` 只计 Recorder 实际取到的样本，包含取到后发现的校验失败和资源拒收；
`received_bytes` 累计取到的、可确认长度的原始 payload，包括出队后的超限拒收。
adapter 在复制/入队前拒绝的单条超限使用 `adapter_oversized_total` /
`adapter_oversized_bytes`，在途预留耗尽时拒绝的新消息使用
`adapter_capacity_rejected_total`；这些不加入 `received_total`。
`oversized_total` 则保留给 Recorder 入口或写队列准入阶段的超限。
新字段对旧序列化数据默认读取为 0；历史数据中的 0 不证明当时实施了字节预算。
`resource_rejected_total` 同样在新版读取旧数据时默认 0；状态、最终报告和 manifest
使用同一个计数字段。旧版读取器可能拒绝它不认识的新字段。

`written_total` 记录已交给 MCAP writer 的样本；文件失败时尚未确认 flush/close 的项另记
`unconfirmed_on_failure_total`，不能据此声称已经持久化。
`source_sequence_samples` 统计携带源序号的样本，本期不据此推导端到端丢样；
`end_to_end_lost_total` 保持 `null`，不把各层的 drop 相加作为端到端证明。
关闭前观察到的 adapter 剩余队列使用 `adapter_queued_at_close_total`，是并发接收下的下界；
writer 队列放弃的项使用 `discarded_on_close_total`。

默认 recorder QoS 为显式 `keep_last(256) / best_effort / volatile`。Action status 默认使用现有 status preset，用户可逐端点覆盖。所有有效 QoS 都进入 Channel metadata 和状态输出。

## 时间与每条消息的证据

writer 的 `log_time` 使用录制开始时的 system anchor 加 recorder ingress 的 steady elapsed，按全局接收顺序分配序号。adapter delivery time 另外保存，不拿 writer dequeue 时间冒充接收时间。分片共用 recording id、锚点和全局序号。

MCAP Message 的 `sequence` 为 u32。全局录制序号使用 u64，Channel metadata 的 `aos.sequence_epoch` 保存高 32 位，Message.sequence 保存低 32 位。epoch 改变必须换 channel，读取时恢复完整序号，不静默回绕。过大或写队列拒收的样本仍消耗录制序号，因此出现间隙是合法的。

每个业务 Channel 有标记 `aos.channel_role=sample_metadata` 的配套 Channel，按完整录制序号保存收到的 `ReceiveMetadata` 和 `SourceMetadata`。它使用自己完整的 Interface/hash，属于 bag 内部证据，播放器不在 AOS 网络上发布它。业务 Message.data 仍是原字节。两条 Message 必须位于同一 CRC chunk，且 Channel id、序号、payload 字节数、log_time 和 publish_time 全部对应；跨 chunk 的同序号记录不能配对。

现有 Zenoh HLC 的时间值、clock id、来源和 comparable 标记原样保存为证据；不能直接当作 Unix publish_time。无法核验发布时刻时，`publish_time=log_time` 且 `aos.publish_time_source=unavailable`。本期不从业务 JSON 的任意 stamp 字段推断发布时间。

## 分片、容量和恢复

采用无压缩、有 chunk/message index 和 CRC 的上游 Writer。chunk 大小由 `aos-bag` 在完整消息及配套证据之间控制，并按 steady 周期 flush；上游 `chunk_size` 是超过阈值后才切换的目标值，不能把它宣称成硬内存上限。单条消息、Schema、channel 数、chunk 和 reader record 长度分别设硬上限。

录制目录从 `manifest.json` 的 `recording / incomplete` 状态开始。当前 shard 使用 `.mcap.partial`，只有 Writer.finish 和 flush 成功后才计算 SHA256、改为 `.mcap` 并更新 manifest。manifest 采用同目录临时文件和原子 rename。`clean_close` 说明容器正常关闭，不代表 fsync 级断电零丢失。

按字节或 steady 时长切片。到总容量后停止，绝不删除旧 shard。容量计算包含容器头尾、索引、metadata 和 manifest；writer 维护关闭预留空间。若文件系统错误或预留仍不足以完成 footer，当前片保留为 incomplete，并明确 `capacity_limit` 或 `io_error`，不得报告 clean_close。

停止原因区分 `requested`、`interrupted`、`capacity_limit`、`disk_full`、`io_error`、`context_closed`、`resource_limit`。有限队列未写完时给出剩余计数；资源上限与写失败都不能仅打印日志后继续声称录制正常。

严格 inspect/play 检查 profile/version、header/footer、数据/summary CRC、chunk CRC、Schema 与完整 Interface/hash 一致性。读取必须流式且有长度上限，不把整份 bag 载入内存。默认 play 拒绝 incomplete；如需使用恢复产物，必须显式允许，并保留来源标记。

recover 只读原文件，写入不存在的新目录。恢复以**完整、CRC 校验通过的 chunk**为提交单位；先验证整个 chunk，再接受其中的 Schema/Channel/Message。遇到截断、CRC 失败、未知压缩或资源拒收即停止读取本片；不跳过当前 chunk 后在同片内继续拼接，也不复制未校验消息。未提供 chunk CRC 的记录不能满足“已校验恢复”条件。

Core 的 Interface 编译或消息校验预算拒收保留为 `ResourceLimit`，不改写为
`Corrupt`。recover 将该错误写入对应 `source.error`，保留之前已经验证的完整
记录，再独立验证后续分片；跨片序列和时间顺序检查仍生效。真实损坏或其他
读取错误仍停止后续分片，未读取部分记为 `Incomplete`。例如原始 Writer 保存
的合法 JSON 可能超过本次校验工作量预算，这不证明文件已损坏。

源读取失败和恢复目标失败分别处理。目标分片数、容量、描述资源或写入失败时，recover 返回错误，并在仍可写 manifest 时标记 `failed / incomplete`；不能把目标限制写成 `source.error` 后报告正常结束。若未到最后的源 hash 回读，`verified_unchanged=false` 仅表示尚未确认，不宣称原文件已被修改。

恢复结果始终 `incomplete=true`，包括原文件 hash、失败位置/类型、恢复消息数和丢弃范围；能够读到前缀不等于原录制完整。有效 chunk 中缺失配对的业务消息和 metadata 分别累计 `discarded_orphan_business`、`discarded_orphan_metadata`；损坏 chunk 的记录数不猜测。`unconfirmed_on_failure_total` 表示已交给 Writer、但写失败时尚未确认完整 flush/close 的业务样本数，是否幸存由 recovery 决定。只有 summary/footer 丢失时，可重建索引并保留已通过检查的数据。恢复也由上游 Writer 生成新 MCAP，不自行编码格式。

## 回放 namespace、类型和 QoS

目标必须二选一：

1. 显式独立 namespace，例如 `/replay_a`；普通 `/sensor/data` 的候选名称为 `/replay_a/sensor/data`。
2. 显式 `original_names=true`；状态和最终报告标明该选择。

先加回放 namespace，再使用 core 的名称/remap 规则。独立 namespace 模式在创建 publisher 前检查最终名称仍位于该 namespace；映射逃出边界时拒绝，不能靠绝对 remap 意外进入原流。Action 只映射 base，feedback/status 后缀由 core 生成；不拆开映射某个内部实体。最终名字超过 AOS 长度限制、两个不同 source 被映射为同一目标，均在发布前拒绝。

同名不同 type_hash 保留独立 publisher；不因为名称相同选择“最新”类型。每个回放 channel 都从 bag 中的完整 Interface 建立 typed publisher，逐条校验后发送原始字节。NodeCard 和状态标明这是 replay publisher；反馈/status 发布不创建或推进 provider goal 状态。

QoS 默认使用记录的有效 profile。若同一源存在冲突的 offered profiles，要求调用者按 channel 选择或覆盖；不自行猜测某个 profile 最合适。有效 QoS、选择来源和映射后的 endpoint 在开始播放前可检查。

Action feedback/status 回放只产生观察消息；不调用其 send_goal/get_result/cancel_goal。参数和 lifecycle 事件回放只发布事件，不设置参数或执行状态转换。

## 暂停、倍速、seek 和 clock

播放时间以 steady anchor、bag position、rate 和 paused 状态计算。暂停冻结 position，恢复或改速时先保存当前位置再重建锚点。rate 必须有限且大于零；所有乘除和 deadline 转换检查溢出。播放按 log_time 排序，相同时间按原文件顺序，跨片按 manifest 片序。

控制与发布通过同一个 dispatch barrier。pause 成功后不再提交新业务消息；seek 成功后不会再提交旧 epoch 的消息。已经交给传输或已经进入消费者队列的样本无法撤回，该边界必须在 SDK 文档说明。

seek 参数是 recording 起点后的纳秒偏移，目标包含在内。清空本地读取/调度队列、递增 epoch、移动 reader、重置播放锚点，随后从首条 `log_time >= target` 的消息继续。暂停中 seek 仍保持暂停；Ready 中 seek 仍保持 Ready，直到显式 start，期间不发布 clock。已启动的播放 seek 至末尾可进入 finished。输入文件的消息本身不被改写。

可选 clock 使用 core `clock_interface()`、clock QoS 和 `Clock::update_ros_time` 的现有语义。发布在回放 namespace 内的 clock Topic；消费者通过已有 `/clock` remap 和 `use_sim_time` 订阅，沿用初始化、时间跳变和 Timer 的共同规则。播放调度与控制超时始终用 steady，不等待消费者的 ROS 时钟。

clock 按配置的 steady 周期更新；已启动后暂停时可继续发布同一 position。seek 的本地提交确认之后、继续消息之前发布时钟跳变；控制返回不代表时钟发送或消费者处理成功。若 bag 中还含被录制的 clock，生成回放 clock 与该记录 channel 不能同时发布到同一目标；开始前要求明确选择其中一个，不能形成两个有效时间源。

`subscriber_wait_ms=0` 不等订阅者。正值要求所有选中的业务 publisher 和可选 clock publisher 都匹配，才提交 start；等待用独立 steady 截止时间，总调用预算是该等待时间加 `control_timeout_ms`，加法溢出会被拒绝。等待期间不发布业务或 clock；超时保持 Ready，并在错误中列出未匹配端点。匹配只证明传输层发现，不证明消费者处理。

打开播放器时完整校验并记录各分片的源文件元数据，同时建立有限的 chunk 索引；seek 复用已验证索引，不再全量 hash。每个 chunk 记录业务 channel 集合，稀疏选择可跳过无关 payload；dispatch 每处理或跳过一个 chunk 就重新处理控制请求。一个 chunk 内的 CRC、JSON 和 Schema 校验仍同步完成，底层不可取消的 I/O 也可能延长控制等待。

`ReaderLimits.index_bytes` 默认 256 MiB，按固定系数估算索引预算。跨片相同的 Schema 内容和 `ChannelInfo.id` 只保留一份描述符及编译后的 Interface；本地 MCAP ID 只映射到这些共享对象。计费为：唯一描述符序列化字节的 4 倍、每个唯一 channel 512 字节、共享 recording header 字节的 2 倍、每片每个 schema/channel 映射 64 字节、每个 chunk 64 字节、每个 chunk 业务 channel ID 2 字节及每片 512 字节。这些系数用于预算准入；JSON Value、编译后 validator 和分配器的实际开销随内容与平台变化，实际索引内存可能高于计费值，`index_bytes` 也不是进程 RSS 上限。`StorageOptions::playback_index_upper_bound()` 公开的是同一计费规则下的写端上界；启动时要求该计费上界不超过 `limits.index_bytes`，默认写入配置可被默认读取配置打开。

回放和恢复仅为当前读取保留文件句柄，不随 shard 数累积；不修改调用进程的 fd 限额。每次重新打开都使用 no-follow，并核对之前记录的大小和 mtime；Unix 还核对设备、inode 和 ctime。一次 hash、完整读取或 chunk 读取在结束时也复核当前 fd 和命名路径，任一已记录字段变化就拒绝继续读取。这是稳定性检查，不是可抵御特权写者的文件快照。恢复期间后续分片打开或最终复核失败时，源报告保留初始 hash 和错误，目标仍正常 finish，保留已经验证的消息前缀。

Windows 目前使用 creation time 和 last-write time，尚未使用稳定文件 ID。`FILE_SHARE_READ` 只在当前读取句柄打开期间排除不兼容的写入或删除访问；两次读取之间没有常驻句柄。如果替换文件的大小和全部已记录时间戳相同，当前检查可能漏检。Windows 的替换检测尚未通过平台验证，不能据 Unix 回归测试认定它具备相同保证。

## 默认资源边界与验证范围

| 项目 | 默认值 |
|---|---|
| 写队列 | 1024 条，payload 与 metadata 合计 32 MiB，drop-newest |
| 单条输入 | payload 1 MiB，metadata 64 KiB |
| chunk | 4 MiB，最长 1000 ms 刷新，不拆业务/metadata 配对 |
| shard | 256 MiB 或 600000 ms；最多 65536 条业务消息、1024 个 chunk |
| 总容量 | 4 GiB，包含文件和 manifest 原子替换预留；达到后停止 |
| reader | 单 record 8 MiB；Schema 1 MiB；descriptor 合计 16 MiB |
| playback index | 256 MiB 估算计费预算；相同跨片 descriptor 只计一次 |
| recording | 4096 个 MCAP channels（含配套 channel）、256 个 shards |
| 控制 | 2000 ms；Player 命令队列 32 条，满时显式拒绝 |

存储测试覆盖原字节与每样本 metadata、上游 `MessageStream` 独立读取、跨 shard 和 u32 序号边界、配对与孤立记录、CRC/截断、磁盘满/写错误、总容量、manifest SHA 与路径检查。队列测试分别验证条数和字节上限。故障注入属于本地测试，不代表真实磁盘故障或断电验收。

调用方设置的读取上限和文件损坏分开报告：例如有效 recording 超过 `ReaderLimits.shards` 时返回 `ResourceLimit`，不能因本次预算较小而将源文件标为 `Corrupt`。

真实 Zenoh loopback 测试覆盖动态同名多 hash、provider 离线后的 inspect 和原消费者回放；Action base remap、feedback/status 与参数/lifecycle 事件回放不改变原 provider 参数和状态；真实消费 Node 的 ROS clock 初始化、暂停冻结、seek 后退和 Timer；阻塞 publish 时 pause/seek 超时、失效命令不在稍后执行；namespace、QoS 冲突、clock 冲突和同目标碰撞。这里的“真实”指测试创建了独立 Zenoh sessions、网络端点与 AOS 消费者，不是跨机器验证。

边界回归还覆盖 adapter 接收时间和 source timestamp 倒退时保留原证据、仅用 ingress 序号与 steady elapsed 确定录制次序；publish 阻塞时 `stop` 能返回、`wait` 必须超时，释放后只完成已进入的那次发布；外部关闭 Context 后 ready/paused/待发消息中的 Player 均收敛为 `failed / closed`，Recorder 则把已接收样本排空并记录 `ContextClosed`。不可取消的底层文件系统调用仍可能延迟实际清理，等待超时不证明 I/O 已终止。

多分片回归构造 32 个分片、256 条样本，覆盖同名 2 种 type hash、2 种 offered QoS 和 u32 sequence epoch 切换；跨分片 seek 后，实际消费者收到剩余 192 条原字节，单 worker 的发布次序也被核对。这是有限规模的正确性测试，不是吞吐或长稳结论。启动配置测试同时覆盖 launcher 的 Node 身份/remap、数据与 clock 的 namespace 逃逸以及同来源字段冲突；所有预检失败都发生在 Node 网络声明前。

CLI/Python 已接入同一 Rust 运行层。安装后的定向测试覆盖真实发布/录制/原类型回放、GIL 释放、SIGTERM 收尾、输出断管和写满、交互控制错误、EOF 停止，以及不依赖网络的检查和恢复。平台支持与部署边界见[发行说明](../releases/v0.2.0.md)和[支持范围](../support.md)。

Rust `*_with_check` 控制和报告等待在不持有状态锁时执行调用方检查，等待切片最多
50 ms，沿用同一请求和 steady 截止时间。Player 启动的订阅者匹配等待也会检查取消
与 stop，不会把调用方已经取消的 start 留在 worker 中长时间阻塞。该切片限制仅指
等待检查间隔，不承诺任意底层文件或传输调用都能在 50 ms 内返回。
