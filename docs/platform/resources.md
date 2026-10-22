# 资源与等待边界

本页说明资源上限、等待和关闭行为，并列出相应回归测试入口。
反复建销后的线程/FD 收敛与完整负载基线仍需各自证据。

## Adapter payload 字节上限

Zenoh adapter 默认允许单条 payload 至多 **16 MiB**、每个接收队列中的 payload 与
在途复制预留合计至多 **256 MiB**。计数单位是序列化后的原始 payload 字节。
`QueueLimits { message_bytes, queue_bytes }` 要求
`0 < message_bytes <= queue_bytes <= 1 GiB`；构造会话或显式声明端点时验证该条件。

这两个数是本地资源策略，不写入 QoS 或 Graph，也不参与匹配。一个会话内多个队列各自
持有预算，因此不能把 256 MiB 当成进程内存总上限。Zenoh 网络缓冲、可靠传输与历史缓存、
图元数据、已出队的 Request、反序列化值、应用持有的数据以及 Action 结果缓存均不包含在
这个队列预算内。

| 接收路径 | 检查与持有范围 |
|---|---|
| 普通与 typed Topic，包括 Action feedback/status | 在 `ZBytes` 转为自有 payload 前检查长度并预留容量；队列持有 `Sample<Vec<u8>>` |
| Service 请求，包括 Action 的三个 Service | 在保留 Query 前检查原始 payload 长度；出队后才生成 Request 的自有 payload |
| 持久 Client 的 completed replies | 并发调用共用一个字节预算；复制前预留，取消、超时移除、出队与关闭释放对应条目 |
| 旧 `call` / `call_cancelable` / `call_wait` helper | 单次调用的回复队列同样执行字节准入，不能绕过限制 |
| 本地 publish、发起请求与发送业务回复 | 提交给 Zenoh 前检查单条 payload；超限不会自动重试 |

队列仍按 KEEP_LAST 淘汰最旧条目，但需要同时满足条数和字节限制，一次入队可能淘汰多条。
单条超限会拒绝新条目，不淘汰旧条目。如果正在复制的条目已经占满容量，新条目也会被
拒绝；回调不会等待预算归还。复制使用的预留由 RAII guard 归还，包括构造失败和关闭竞争。
淘汰的 Query 在队列锁外回复和释放。

Service 请求被某个 provider 以 `SERVICE_REQUEST_TOO_LARGE`、
`SERVICE_REQUEST_CAPACITY` 或 `SERVICE_QUEUE_OVERFLOW` 拒绝时，该拒绝先保持为候选结果；
其他 provider 的成功回复仍可胜出。真实调用截止时间仍返回 Timeout。全部 provider
结束后才采用已记录的拒绝原因，客户端不因拒绝重发请求。
新增拒绝码沿用版本化的 v1 adapter 错误编码；旧 SDK 若不识别新码，会把它作为
Remote 错误返回，不会因此获得重试许可。

回复在客户端超限会以 `REPLY_MESSAGE_TOO_LARGE` 或 `REPLY_QUEUE_CAPACITY` 结束当前等待。
Provider 在发送业务回复前发现超限时发送固定的 `SERVICE_REPLY_TOO_LARGE` 协议错误。
这些情况都不能证明业务没有执行。固定的 adapter/middleware 控制错误不计业务 payload；
普通远端错误保留原始字节，出队后才转换字符串，避免非法 UTF-8 在队列内膨胀。

`QueueStats.bytes` 返回以下附加观察值；旧 adapter 返回 `None` 表示没有这项证据，
不能理解为已实施上限或当前占用为零。

| 字段 | 含义 |
|---|---|
| `message_limit_bytes` / `queue_limit_bytes` | 当前队列实际使用的两个上限 |
| `queued_bytes` / `reserved_bytes` | 已入队 payload / 当前复制预留；两者之和不超过队列上限 |
| `received_bytes_total` | 成功入队的累计 payload 字节 |
| `evicted_bytes_total` | KEEP_LAST 淘汰的累计字节；不包含出队和关闭清空 |
| `oversized_messages_total` / `oversized_bytes_total` | 单条超限的次数和原始 payload 字节总量 |
| `capacity_rejected_total` | 在途预留已占满容量导致的拒绝次数 |

原 `received_total` 继续统计成功入队数，`dropped_total` 继续统计 KEEP_LAST 淘汰数。
关闭后已有复制可能尚未返回，因此 `reserved_bytes` 会保留实际预留，直到复制退出；
关闭会立即清空已入队条目，并唤醒等待者。

Rust 可通过 `ZenohTransport::open_with_queue_limits` 或
`open_with_telemetry_and_queue_limits` 配置整个会话。单个接收端可使用
`Node::create_subscription_with_limits` 或
`create_action_observation_subscription_with_limits` 覆盖该端的上限。显式预算要求
adapter 实际支持，旧自定义 adapter 返回 Unsupported，不回退为普通声明。

原 Rust/Python 创建入口保留，并使用 Zenoh 的默认上限。本次新增的显式配置入口位于
Rust adapter/Node API，Python `Context` 签名不变。以前可发送超过 16 MiB payload 的程序
现在会收到超限结果；需要更大 payload 的 Rust 程序须显式配置预算。旧序列化
QueueStats 仍可读取；Rust 代码若直接构造 `QueueStats` 字面量，需要补充 `bytes: None`
或实际字节观察值。

## Action 观察与结果等待者

Action client 的 UUID 索引只弱引用当前仍有 `ClientGoalHandle` 或 SendGoal 调用持有的
状态观察。复制 handle、同 UUID 的多个 handle 共享观察；最后一个 owner 释放时移除
对应索引。SendGoal 发出前就建立观察，因此先于回复到达的 provider status 不会丢失。
GetResult 只更新已有观察，不为没有 handle 的历史 UUID 建立永久缓存。

观察仍只来自 provider 的 status 或 GetResult 回复。某个 provider 的完整 status 快照
没有另一个 provider 的 goal，不能据此删除该 goal 的存活观察。同一 UUID 的全部 owner
释放后，重新创建的 handle 先报告 UNKNOWN，直到收到新的 provider 证据；不会继承旧缓存。
这不改变 provider 对 goal 状态机的所有权。

Python feedback callback 也使用弱索引。正在发送的 SendGoal 和已接受的
`ClientGoalHandle` 持有同 UUID 的注册，即使第一次发送没有提供 callback。后续发送
失败或拒绝只释放自己的 owner，不删除其他存活 handle 或发送请求仍持有的 callback。
最后一个 owner 被释放或由 GC 回收后，callback 和索引一起释放。回调引用 handle
形成的循环也可以由 GC 回收；应用继续持有的对象不算 SDK 历史缓存。

GetResult 超时或 Remote 错误保留仍有 owner 的 callback，也不取消 goal。成功取得
结果会释放该次观察对应的 callback；晚到的旧结果不能删除同 UUID 的新注册。查询
Future 中保留的错误 traceback 不额外持有 Python goal handle。销毁 client 会释放
callback，即使 native 资源已经关闭。Executor 已取走的 invocation 可完成，异常、
KeyboardInterrupt 或提交取消仍通过现有 `finally` / `_cancel()` 归还 native lease。
这些规则只管理 Python 回调所有权，不增加 Python goal 状态机。

`ActionServerOptions.max_result_waiters` 与 Python `ActionServer(...,
max_result_waiters=1024)` 限制**一个 server 内所有 goal 合计**保留的待处理 GetResult：
默认 1024，允许整数 `1..65536`。达到上限后，新请求收到带固定
`ACTION_RESULT_WAITER_LIMIT` 代码的 Remote 错误，已有等待者不被驱逐，goal 不取消、
不跳转状态，SDK 不自动重发。已有缓存或未知 UUID 的即时回复不占这个等待预算。
这是 provider 的请求保留策略，不表示 transport 队列溢出。

每个保留请求持有一个 RAII permit。结果完成或错误、过期、server 关闭都会取走相应请求；
permit 在回复返回后释放，包括回复失败。正在发送的回复仍占一个 permit。客户端本地
超时或关闭不会伪造 provider 的取消事件；当前 RequestBackend 没有这类对端通知，因此
该请求继续计数，直到 provider 完成、过期或关闭。结果保留时间 `result_timeout` 的语义
不变，仍只让 terminal goal 过期，不给 active goal 强行添加执行截止时间。

Rust 直接构造 `ActionServerOptions` 字面量的调用者，需要增加 `max_result_waiters`、
`resource_limits` 或使用 `..Default::default()`；Python 新参数都是可选的 keyword-only
参数。观察索引、provider 保留数据和调用者自行发起的并发等待使用各自的边界。

回归先在旧实现上执行：3 项 client 所有权断言与 1 项全 server 等待上限断言失败，
原有关闭行为 1 项通过；修复后这些原断言不变。补充测试覆盖同 UUID 的最后释放竞争、
不同 provider 快照、早到 status、失败发送、完成与入队竞争、关闭与入队竞争、过期、
失败回复以及回复阻塞期间的 permit 持有。它们不是性能测试。

## Action provider 的 goal 与结果保留

Rust `ActionServerOptions.resource_limits` 使用 `ActionResourceLimits`；
Python `ActionServer(..., resource_limits={...})` 接受映射。省略配置、传入 `None`
或省略某个字段时使用对应默认值。所有字段须为正整数，未知字段、布尔值、浮点数和越界值
均拒绝；Rust/Python 共用 Core 的范围验证。

| 字段 | 默认值 | 允许范围 | 计数对象 |
|---|---:|---:|---|
| `max_goals` | 1024 | 1–65536 | 已取出的待审批请求与 registry 中的 goal 合计 |
| `max_goal_bytes` | 1 MiB | 1 byte–8 MiB | 单个 goal 业务值的 JSON UTF-8 字节 |
| `max_result_bytes` | 1 MiB | 1 byte–8 MiB | 单个 result 的 JSON UTF-8 字节，或业务错误文本的 UTF-8 字节 |
| `max_result_cache_bytes` | 64 MiB | 1 byte–1 GiB | 本 server 保留的结果与业务错误文本合计，包括仍被回复使用的共享值 |

`max_result_cache_bytes` 还必须不小于 `max_result_bytes`。例如：

```python
server = aos.ActionServer(
    node, interface, "process", execute,
    resource_limits={
        "max_goals": 128,
        "max_goal_bytes": 256 * 1024,
        "max_result_bytes": 512 * 1024,
        "max_result_cache_bytes": 8 * 1024 * 1024,
    },
)
```

goal 超过单条上限或共享名额已满时，SendGoal 在用户 `goal_callback` 之前返回
`accepted=false`，不执行新 goal。名额在交给审批回调前预留，接受后转交给 registry；
拒绝、丢弃请求或决策失败归还其预留。已被接受的 goal 不因回复发送失败而回滚，
也不会因资源不足被驱逐。这样的已登记 goal 继续占名额，直到按原协议完成、过期或
server 关闭。active goal 没有强加 TTL。

结果先通过有界计数器计算大小，再预留总字节额度，之后才复制到内部缓存。超过单条
上限时使用固定的 `ACTION_RESULT_TOO_LARGE`；总预算不足时使用固定的
`ACTION_RESULT_CACHE_LIMIT`。被拒绝的业务值不进入缓存，固定错误作为小型控制信息
随 goal 保留，其条数由 `max_goals` 限制。已有未过期结果不被提前淘汰。

结果超限保留已经确定的 `SUCCEEDED`、`CANCELED` 或 `ABORTED`，provider 的
`finish` 返回相应 Remote 错误，当前和后续 GetResult 返回同一错误。该错误不表示
业务未执行，不触发取消或自动重发。execute 返回时仍为 active 的 goal 继续使用原有
的 ABORT 规则；资源限制不新增状态机边。schema 校验失败、不可编码返回值及默认结果
同样受字节限制，结束处理仍释放 managed Action 的执行 lease。

TTL 到期或关闭 server 会清除内部结果缓存和 registry 名额，即使应用仍持有
`ServerGoalHandle`。发送中的回复通过共享所有权继续占用该结果的字节预留，直到回复
成功或失败返回；缓存释放不会让这些字节提前从预算中消失。已交给应用的待审批请求由
应用持有，关闭后仍要决定或丢弃该对象，才能归还其名额和 lease。

上述字节是保留业务数据的序列化大小，**不是进程 RSS 上限**。固定协议元数据、类型
定义、transport 队列、编码回复期间的临时缓冲以及应用自己保留的 request/result 不
属于该预算。应用持有的 `ServerGoalHandle` 仍可保留其 request；SDK 不替应用撤销
这个所有权。UNKNOWN goal 的即时默认结果受单条结果上限检查，但不进入结果缓存。
有限的名额、业务字节和接收队列上限不能替代应用自身的并发控制。

原创建入口仍可调用；行为变化是原来无限保留或接受的数据现在使用默认上限。Rust
结构体字面量须补 `resource_limits: ActionResourceLimits::default()` 或使用
`..Default::default()`。需要更大数据的调用者应显式配置允许范围内的预算，并同时
核对 adapter 单条 payload 上限。

旧实现的六项负向断言覆盖默认共享名额、超大 goal、超大 result、总缓存预算和应用
持有 handle 时的 TTL/close 清理；六项均失败，修复后保留原断言。补充用例覆盖精确
UTF-8 与 JSON 转义边界、错误文本、默认结果、并发完成/关闭、阻塞回复保留预算，以及
真实 Python peer 上的拒绝、终态、重复 GetResult 和 lifecycle drain。这些是有限回归，
不是长稳或吞吐性能证明。

## Interface 编译与校验准入

本地 `Interface::load/from_json`、远端类型发现和语言绑定共用以下资源限制。
字节、结构、引用和正则总量限制在 schema 编译前检查，正则编译大小由同一后端限制；
它们是确定性的准入规则，不是 CPU deadline
或进程 RSS 上限。符合 JSON Schema 的定义或消息仍可能因资源超限而被拒绝。

| 对象 | 上限与计数方式 |
|---|---|
| 完整 Interface | 紧凑 JSON 至多 4 MiB；本地文件原始字节也至多 4 MiB，先限长再解析 |
| 远端类型描述回复 | 原始回复至多 4 MiB；Zenoh 在复制回复前限制，自定义 adapter 返回后 Core 再检查，随后才解码 |
| 每个 message/request/response/goal/result/feedback schema | 紧凑 JSON 至多 1 MiB；这些是各部分限制，不把 Action 的整个描述当成一条 message schema |
| 完整定义的 JSON 结构 | 至多 65,536 个值节点，根深度为 1，最大深度 64；包括 default/examples 等数据 |
| Interface 内 schema 结构 | 至多 1,024 个 schema 节点、128 个 `$ref`/`$dynamicRef`；未使用的定义也计入结构 |
| 单个 root schema 的引用展开 | 至多 1,024 次 schema 访问；每条引用边都累加目标费用，缓存仅省去重复计算，不能免除第二次引用费用 |
| 引用分析 | 每个 root 至多 4,096 个解析状态；状态由实际目标、base URI 和 dynamic scope 共同区分 |
| 正则总量 | 在各 root 内按不同 pattern 字符串去重，再跨 root 相加，最多 16 个、源码合计 64 KiB；`patternProperties` 的 key 同样计入 |
| 正则预算 | 每个正则编译大小至多 1 MiB、DFA 缓存至多 256 KiB；总 charge 为正则数乘两者之和，最多 20 MiB，不包含其他分配，不能作为 RSS 证据 |
| 正则回退程序 | schema 位置中按后端选择规则检查的 pattern，其正向 Thompson NFA 至多 768 个状态；包括未引用定义中的非 literal 模式，在接口构造时检查，不执行 payload 匹配 |
| 单次 payload 校验 | 按值类型的节点费用，加字符串/key 的关键字费用，合计至多 8,388,608 units；在调用 validator 前检查 |

引用解析使用与 `jsonschema 0.59.0` 同版的 resolver，支持本地 JSON Pointer、anchor
和嵌套 `$id` 的作用域。外部引用继续拒绝；循环引用（包括 `$dynamicRef` 回边）现在
明确拒绝，未使用定义里的循环也不例外。`$defs` 只有被引用时才增加 root 展开费用。
递归引用仍归为 `ResourceLimit`：这是当前有限资源 profile 的准入拒收，不能通过增大
payload 或预算启用递归支持，也不表示 JSON Schema 本身非法。
schema 位置的旧式 `$recursiveRef` 也明确拒收：固定 `jsonschema 0.59.0` 的
`unevaluatedProperties` 属性评估源码仍会解析这条入口，有限 profile 不允许它绕过
引用预检。这条路径由源码确认，没有用递归 payload 执行复现。同名数据字段、
default/examples 中的数据保持原样；被引用而成为 schema 后才应用该规则。
结构计数对同一实际对象只计一次，展开费用仍按每条边累加。当前固定上游在 2020-12 下仍编译 deprecated `dependencies`，其 schema 值纳入同一遍历；属性名数组保持数据语义。`default`、`examples`
或未知字段里的对象原本是数据；如果引用实际指向该对象，它会进入同一 schema
节点、引用和正则预算，不能借数据位置绕过预检。

每个 JSON 值节点先按 root 的展开 schema 访问数计费。随后加入关键字费用：

| 校验路径 | 增加的费用 |
|---|---|
| 仅 `type` / `properties` 等结构规则 | 不按字符串内容字节计费 |
| `pattern`、长度、format、content 关键字 | 每个对应关键字按字符串 UTF-8 字节计费；literal alternation 按候选数量计费 |
| 整数 enum，输入可表示为 i64 | 每个数值加 `floor(log2(enum.len())) + 1`；固定上游走二分查找 |
| 同一整数 enum，输入写成浮点或超出 i64 | 每个数值加 `enum.len()`；上游需逐项做数值等价比较 |
| 仅字符串/nullable string 的 enum，超过 10 项 | 每个字符串加 1 次 hash 的字节费用；固定上游使用哈希集合 |
| 小字符串 enum、混合 enum、const | 按候选数计比较费用；复合 JSON 比较同时覆盖后代值、字符串与 key |
| `uniqueItems` | 加入后代值与文本的 hash/比较费用 |
| `patternProperties` | 每个 key 按 pattern 数量计节点和字节费用；`propertyNames` 也覆盖 key 校验 |

这些系数绑定 `jsonschema 0.59.0` 的实现。`enum` 费用独立于 1,024 schema 节点上限，
所以正常的 250 项 enum 不会仅因枚举数量而被拒。各分支系数保守累加到整个 payload；
预检不会判断某次校验实际选择了哪些分支。字符串关键字也保守计入 key 费用。
引用的每条边重复累加全部系数，缓存不减免费用。所有算术均有饱和/剩余预算检查，
超限返回 `ResourceLimit`；预算内仍由正常 validator 校验，并只取得首个错误。

正则编译超限也返回 `ResourceLimit`。上游把 regex 语法错误和编译超限合为一个
`format: regex` 错误；Core 使用同版 `jsonschema-regex 0.59.0` 的 ECMA 转换和
`regex-automata 0.4.18` 的正向 Thompson NFA 构造，按与 `regex 1.13.1` 相同的
UTF-8、capture、非 reverse、非 shrink 配置检查 1 MiB 编译大小及 768 状态上限。
普通 `pattern` 的 Exact、Prefix、Alternation、NoWhitespace 快速路径保持不变。
`patternProperties` 只有整组都是 literal 时才走这一快速路径；组内含一般正则时，
所有项都检查 NFA。`additionalProperties: false` 或 schema 则逐条选择 literal 优化。
`unevaluatedProperties: false` 或 schema 的属性评估会重新使用 regex 后端，预检沿
allOf/anyOf/oneOf、条件、dependentSchemas 和已解析的本地引用检查相关 pattern；
不把父对象的评估传到属性值或未引用定义。`unevaluatedProperties: true` 仍是无操作。
失败的后端编译保留大小拒收分类；语法错误交由正式 validator 报告，仍为
`InvalidArgument`，保留 `invalid message/request/response/goal/result/feedback schema`
中的具体字段名与上游诊断。预检不自行拒绝未被实际编译的非法正则。

768 是有限资源 profile 的兼容取舍，不是耗时保证。只编译核对中，当前业务的五种
正则最多 69 个状态，UUID 为 37，Unicode `^\p{L}+$` 为 304，包含空格、连字符和
撇号的 Unicode 姓名组合为 667；因此没有采用会拒绝该组合的 512 上限。新增边界会
拒绝此前可编译的 `[a-z]{764}`（769 个状态）。`^\p{L}{1,64}$` 已超出原有 1 MiB
编译上限，属于既有拒收。这些数字是固定依赖的构造结果，没有执行大输入匹配。
NFA 状态数不乘进全局字节费用；Image 与 UUID 组合仍只增加对应 pattern 的一次
字节费用。状态数和编译大小都满足上限的正则，也可能受其他共享预算限制。

这不是指令数、CPU deadline 或 RSS 保证。正则 DFA 缓存失效、关键字实现、hash
碰撞、分配和 OS 调度都可能改变耗时；每条消息的准入也不限制多端点的发送速率。

所有 Interface 资源预检和 payload 准入拒收使用 Rust `Error::ResourceLimit`。
Python 对应 `ResourceLimitError`，继承 `InvalidArgumentError`，保留旧调用方的捕获方式；
CLI 对应 `RESOURCE_LIMIT`，退出码为 2。错误参数和非法 schema 仍为 `InvalidArgument`，
预算内的 schema 不匹配仍为 `Validation`。Core 接口解码产生的单条拒收不会停止
时钟、lifecycle 查询、Action 结果及 inactive goal worker；内省 worker 回复解码错误后
继续接收。参数服务与 Action client 的同步接收 API 把错误交给调用方；Python Executor
按现有 `InvalidArgumentError` 捕获方式继续轮询。CLI echo 遇到资源拒收会结束本次命令，
launch readiness 则报告 `ReadinessFailed`，二者不属于自动吞掉错误的后台 worker。

Rust `Context::get_type_description_bounded(..., TypeDescriptionLimits)` 可以进一步
收紧 `response_bytes` 和 `schema_bytes`，不能放宽上述共享上限。
`Interface::from_json_bounded(definition, schema_bytes)` 同样只允许收紧各部分字节上限。
Action 父接口还会检查实际生成的 SendGoal、GetResult、CancelGoal、Feedback 和 Status
接口，包括 wrapper 的字节、节点、引用及正则开销；父接口被接受后，这五个派生接口
必须能在共享 profile 下构造。调用方进一步收紧的 `schema_bytes` 只约束输入定义的各
部分，内部 wrapper 检查使用共享上限。派生检查直接调用同一编译器，不递归调用公开
`from_json`，不会重新进入 Action 父接口检查。

默认入口保持原签名。`Transport::call_bounded` 的默认实现兼容旧 adapter，但只能在其
返回 `Vec` 后拒绝；只有原生 Zenoh 路径在 payload 复制前拒绝。类型哈希仍只取定义的
RFC 8785 规范化内容，不加入本机阈值。

一组边界回归使用 `items.allOf` 的 1,022 个分支：其展开费用为 1,024，数组包含
8,191 个整数时恰好达到预算；8,192 个整数即被提前拒绝。简单对象内的 256 KiB 字符串、11 节点 Image schema 的 819,200 字符 base64 数据
及仅 `type:string` 的 8 MiB 以上字符串仍通过。16 个 pattern 的大字符串和大 key
仍按对应正则数量计费。它们是兼容与正确性控制，不能据此宣称所有已接受 schema 都满足某个延迟上限。

## Python 主线程无限等待

以下三个公开入口在等待期间，每隔至多 50 ms 的本地等待步骤检查 Python 信号：

| 入口 | 中断后的资源状态 |
|---|---|
| `Client.wait_for_service(None)` | 同一 client 仍可等待原服务；本次 discovery watch 释放 |
| `ActionClient.wait_for_server(None)` | 同一 Action client 仍可使用；本次 watch 释放 |
| `Rate.sleep()` | 同一 Rate 仍可使用；尚未到来的周期不被消费，模拟时钟暂停也可中断 |

`None` 仍表示没有应用截止时间。检查发生在调用者线程，并在资源、图和时钟锁外运行。
50 ms 是等待步骤上限，不包含 OS 调度或用户信号处理函数本身的执行时间。
实现不创建替代线程、不重发业务请求，也不通过关闭整个 client 来模拟中断。

Core 的 `wait_for_service_with_check`、`wait_for_server_with_check` 和
`sleep_with_check` 允许调用者提供非阻塞检查。检查报错只结束当前等待；检查函数若主动
关闭资源，则保留该关闭结果。Python 绑定先保存信号产生的异常，再用内部返回值退出
Core 等待，因此信号处理函数先关闭 Context、再抛出 `KeyboardInterrupt` 时，
该 Python 异常不会被普通的资源已关闭错误覆盖。

等待可用性期间只创建一个持久 Graph watch。检查信号不会重新声明 discovery interest，
也不会把无限等待改成一串短期网络查询。旧自定义 adapter 的 snapshot fallback 保持原行为。

## 其他等待入口的审计范围

| 入口 | 本次确认的边界 |
|---|---|
| 公开 Action `get_result_async().result()` | 主线程等待 Future 可被 SIGINT 中断；Future 和 provider goal 不被隐式取消，原结果仍可随后完成 |
| `Executor.spin_once(None)` | Python 的有限 Event 等待允许 SIGINT；中断后可继续 spin |
| `MultiThreadedExecutor.shutdown(None)` | 等待已有用户回调退出时可被 SIGINT 中断；回调不被强制终止，释放回调后可继续完成 shutdown |
| 私有 native `_ActionClient.get_result(None)` / `_ClientGoalHandle.get_result(None)` | 保留供后台结果线程使用的 native 等待；本次不承诺直接在主线程调用这些私有入口可响应 SIGINT |
| 有限超时的 native call/recv、Context 建连与关闭、bag 控制 | 不能由上述三个入口的测试推导其全部行为；各自保留专门验收 |

## 回归入口

```sh
cargo test -p aos-core --test wait_checks
cargo test -p aos-core --lib clock::tests
cargo test -p aos-core --test queue_limits
cargo test -p aos-core --test interface_resources
cargo test -p aos-core --lib interface_limits::tests
cargo test -p aos-zenoh --lib bounded_reply
cargo test -p aos-zenoh --lib
cargo test -p aos-zenoh --test queue_bytes
cargo test -p aos-zenoh --test default_payload_limit
cargo test -p aos-core --lib resource_tests
cargo test -p aos-core --lib action::
python -I -m unittest discover -s python/tests -p test_platform_resources.py -v
python -I -m unittest discover -s python/tests -p test_action_resources.py -v
python -I -m unittest discover -s python/tests -p test_action_feedback_ownership.py -v
python -I -m unittest discover -s python/tests -p test_action_server_limits.py -v
```

Python 命令须使用待测 wheel 的隔离安装环境。它创建独立 loopback peer 和测试子进程，
只向该子进程发送 SIGINT，并在失败时回收该进程。用例验证无限等待实际进入主线程、
资源复用、关闭与信号竞争，以及 Action 结果等待中断不会取消 provider goal。
这些是有限正确性回归，不是性能阈值或 24 小时长稳证据。
