# 从 AOS 0.1.0 迁移到 0.2.0

0.2 开发期间多个候选暂时共享版本号。制品身份以源码 SHA 和文件 SHA256 为准。
每次安装验收使用新虚拟环境；在已有环境更换候选时必须使用
`python -m pip install --force-reinstall --no-deps /path/to/verified-wheel.whl`，
并核对已安装文件与 wheel RECORD，不能仅凭 `__version__` 或 pip 的“已安装”提示判断升级成功。

本文记录相对 0.1.0 基线 `07d36ef86974fa0407bcace8dfc621a7c82daac0` 的已实现行为变化。当前发行资产、平台及原生验证范围见[发行说明](../releases/v0.2.0.md)。历史候选即使版本号相同，也必须按实际源码和制品摘要区分。

## 脚本和 SDK

| 0.1.0 行为/边界 | 0.2.0 行为与迁移动作 |
|---|---|
| Topic pub 已默认一次，且基线会先等待至少一个匹配订阅者 | 保持默认一次，改为默认不等订阅者；需要旧等待行为的脚本显式写 `wait_for_subscribers=1` 和 `match_timeout`。pub 退出码仍不表示消费完成 |
| Topic echo 默认有限窗口、follow 显式 | 保持 30 秒默认；新增 count、idle_timeout、filter、fields、hook。需要无限观察的脚本写明 follow；如同时给 timeout，则仍有限 |
| Python Topic 支持 QoSProfile；CLI 不提供完整覆盖 | 旧默认保持 depth10/reliable/volatile；新入口返回 effective QoS。未知/高级策略明确报错，删除靠静默忽略工作的配置 |
| Service/Action QoS 多为固定内部值 | 显式配置每个 Service 和五个 Action 通道；Service 仅 reliable/volatile，可改 depth。status 默认仍为 depth1/reliable/transient_local |
| 仅名称展开 | 新增部署覆盖与精确 remapping。程序 NodeOptions 是可覆盖初值；需要固定 namespace 的部署应在最终配置中明确指定 |
| `Context(config=..., domain_id=...)` 原本优先于环境 | 统一采用六层配置顺序，这两个参数属于程序初值，环境可覆盖；需保持调用方最高优先级时改用 `cli_overrides={"zenoh_config": "...", "domain_id": 72}` |
| 无公开 Clock/Timer/Rate 完整平台 API | 可保留原业务循环；迁移 Timer 时明确 clock。控制 timeout 不可改用模拟 clock |
| 参数/lifecycle/run/launch/bag 未形成平台入口 | 新能力显式启用；普通 Node 不会仅因升级而变成 LifecycleNode 或自动被监管 |
| Action GetResult 使用协议长等待；普通 timeout 不取消 | 保持该边界；本地取消等待与 provider CancelGoal 分开，接受取消不等于已 CANCELED |
| 开发 peer/TCP 配置 | 只继续用于开发；正式安全部署切换明确的 mTLS client/router 配置，禁止自动回退 |

`once/follow/times` 的重复模式在新版本提前报错，旧脚本中冗余的 `once=true,times=1` 需只留一个。pub 旧 `timeout` 保留为类型发现/显式匹配等待上限的别名，不能与新 `discovery_timeout/match_timeout` 混用，也不会隐式开启匹配等待。Service payload 字段保持 `values`。SDK `publish()` 继续一调用一条；CLI 的循环调度不进入 SDK 的单次语义。现有 Action 的整数状态码、CancelGoal 选择规则、JSON Interface 和 type_hash 算法保持；新增服务不会让旧 provider 自动具有参数、lifecycle 或新 QoS 能力。

所有本地与远端 Interface 的 JSON Schema 统一使用线性时间正则引擎。
`pattern` 与 `patternProperties` 不再接受 look-around 或反向引用；依赖这些语法的
0.1.0 Schema 需要改写后才能注册、发现或用于 bag。每个正则的编译大小上限为
1 MiB，DFA 缓存上限为 256 KiB；这些是单个正则的限制，不代表整个 Schema
或进程的内存上限。实际使用 regex 后端的正向 NFA 还须不超过 768 个状态，
超限在接口构造时返回 `ResourceLimit`；此前可编译的大重复表达式可能因此被拒收。
literal 快速路径保持，但属性评估重新使用 regex 后端时仍须符合此上限。
该限制不是单次调用的耗时保证。Interface 的规范化与 type_hash 算法保持不变；实际改写 Schema
后应按新内容重新计算 hash，不能沿用旧 hash。

Interface 现统一实施编译前的字节、深度、节点、引用展开和正则总量限制，详见
[资源准入规则](resources.md#interface-编译与校验准入)。递归 `$ref`/`$dynamicRef`
现在明确拒绝，包括未使用定义里的循环；需要递归数据的接口须先改为有限深度表示。
schema 位置的旧式 `$recursiveRef` 也拒收，防止固定上游的属性评估绕过引用预检；
default/examples 等数据里的同名字段保持原样，除非被引用为 schema。
这一准入拒收返回 `ResourceLimit`；当前 profile 不支持递归，调整 payload 大小或
其他预算不会启用它。合法但被此 profile 拒收的 schema 不视为源文件损坏，
Bag 恢复仍会独立检查后续分片。
每次消息校验的节点与关键字费用合计必须不超过 8,388,608 units，超限返回
`ResourceLimit`。字符串只为触碰内容的关键字计字节费用；11 节点 Image 类型的
819,200 字符数据恢复兼容。整数 enum 区分 i64 二分查找和浮点逐项比较，复合
JSON enum/const 也计入后代值和文本。正常 250 项 enum 保持兼容。本地、远端、Python 使用
同一规则，不能通过其他入口绕过。单次类型描述的原始回复字节超限仍返回 `QueueOverflow`；
Interface 定义和 payload 资源超限统一返回 Rust `Error::ResourceLimit`，Python 对应
`ResourceLimitError`（继承 `InvalidArgumentError`，兼容原有捕获），CLI code 为
`RESOURCE_LIMIT`、退出码为 2。错误参数和非法 schema 仍为 `InvalidArgument`，
预算内的 schema 不匹配仍为 `Validation`。Action 父接口会同时检查实际派生 wrapper，
接近上限的旧 Action 可能在加载时就被拒收；`dependencies` 内的 schema 也纳入预算。
正则编译超限改为 `ResourceLimit`，语法错误仍保持原分类。
默认 API 签名和 type_hash 算法保持不变。

通过 `Context::get_node_card` 获取的远端 NodeCard 在反序列化前检查原始回复大小，
上限为 1 MiB，超限返回 `QueueOverflow`。Rust 的 `get_node_card_bounded`
只允许收紧该上限。CLI node info 与 Bag 使用此入口；launch readiness 的 Card
探针仍用普通 Client，受 adapter 通用回复预算约束，没有这项专用 1 MiB 限制。
旧程序若把大量描述或其他大字段放在 NodeCard 中，需要缩减卡片；
这与 Interface 的单次类型描述字节限制分别执行。

在线 CLI 自身使用唯一的临时节点名，不继承 provider 的 `AOS_NODE_NAME` 或
`AOS_USE_SIM_TIME`。`AOS_NAMESPACE` 只影响接受端点名的命令；
`node_config` 和 `declared_name/declared_namespace` 控制目标名称展开、选择和 remap，
不会重命名 CLI 的内省服务。离线 `config resolve` 仍检查完整 Node 环境。
SDK 的 `AOS_NODE_NAME` 会覆盖同一进程后续新建的每个 Node；多 Node 进程需用
更高优先级的 launch/CLI 专属配置区分身份，避免同名内省服务。

Service 的明确队列溢出与截止前无回复分别使用 `QueueOverflowError`、
`NoResponseError`，均继承 `CommunicationError`；真正到期仍是 `WaitTimeout`。
只捕获 `WaitTimeout` 的旧程序应补上这两类本地等待失败，并保持“不自动重试、不推断
provider 未执行”的业务边界。CLI 对应错误码为 `QUEUE_OVERFLOW/NO_RESPONSE`：
这些明确溢出或提前结束的情况原先可能返回 `TIMEOUT`、退出码 4，现在返回退出码 3。
真正达到请求 deadline 仍返回 `TIMEOUT`、退出码 4；按退出码分支的脚本需同步调整。

`QoSProfile.preset` 的类型由 `str` 改为 `str | None`。省略 preset 时为 `None`，
表示使用创建实体的默认预设；例如 Action status 仍用 transient_local。
`effective_config` 是不绑定实体的 Topic 默认预览，可能显示 `preset="default"`；
最终值应读取已创建实体的 QoS。需要明确指定通用预设时写 `preset="default"`。

Service 未提供 `times` 时仍返回单个 JSON；显式提供 `times`（包括 1）时改用
JSONL `response` 事件和最终 `summary`。每次调用是新的业务请求，首个失败终止循环，
不能把这些显式调用当作幂等重试。CLI 响应里的本地调用编号与遥测 wire 请求 ID
有不同 scope，关联跨进程记录应使用后者。

观察的 `fields` 输出是 JSON Pointer 到值的对象；hook 仍接收完整消息。
`summary.filtered` 表示通过过滤的数量，而不是被丢弃的数量。
echo 总时限到期返回 4；hz/bw/delay 正常结束有限测量窗口返回 0，
任意观察空闲时限到期仍返回 4。doctor 生成报告后可返回 0，
即使报告中的 `data.status` 为 `error`；脚本必须检查子项状态。
日志默认不输出，显式开启后写 stderr，业务 stdout 格式不变。
详见 [CLI 使用指南](cli-observation.md)。

## 版本和混合部署

软件版本、CLI 请求 Schema、Interface/contract_version、传输 key 和 bag format 是不同版本。0.2.0 文档使用 MCAP `aos-json-v1`，不把软件小版本直接用作 bag 格式版本。核心 type_hash 仍对完整 Interface 按 RFC 8785 规范化后做 SHA256；新增一个描述字段也可能改变哈希，不能只比较类型名。

本期正式保证同一候选的 0.2.0 SDK/CLI/router 配置组合。与 0.1.0 的混合版本 Topic/Service/Action 尚无完整兼容保证；部署时统一使用 0.2.0。对旧端点缺失的 QoS/能力字段返回 unknown/unsupported，不猜默认，不把新请求悄悄降级。router 保持固定 Zenoh 1.10.1，只有兼容验证和独立提交才能换版。

## 升级与回滚的发布门槛

1. 记录旧 CLI/SDK/zenohd 版本、配置、入口资源和制品摘要；备份到带时间与版本的独立目录，保留权限。
2. 在独立 Python 环境及制品目录安装同一 0.2.0 候选；先运行 doctor 和无副作用读探针，再切换 supervisor。不能把旧环境原地覆盖作为唯一升级路径。
3. 校验有效配置、名称与 key、Schema/hash、QoS、证书和 ACL；先完成本机，再做 Mac/Linux 双向业务及取消。已有进程关闭前完成 drain；剩余业务由其业务恢复契约处理。
4. 验证失败时停止新 supervisor，恢复旧版本入口与旧配置；不把新默认字段强写回旧文件，不删除用户 bag、业务数据或证书。
5. 回滚后重新做发现和业务探针。进程重启不恢复旧内存 goal；不能把 UNKNOWN 当成任务从未执行，业务侧按已有幂等/恢复规则处理。

三平台下载、Python/OS/架构范围、SHA256、Router 与许可入口统一在[安装指南](../install.md)、[支持范围](../support.md)和[发行说明](../releases/v0.2.0.md)维护。平台能力只按实际原生安装和业务结果声明；历史候选的成功不能代替新包的升级/回滚验证。旧 MVP 文档保留原来源。

## ROS 2 拓扑对齐的兼容变化

默认 peer 现在在本机 Router 缺席时也能打开，后续后台连接；开发 Router 默认监听 `[::]:7447`，与 rmw_zenoh 的多机入口一致。依赖启动失败来判断 Router 是否在线的程序，应改为检查业务就绪，或显式配置 `connect.timeout_ms` 与 `connect.exit_on_failure=true`。需要仅本机可达时，显式使用 loopback listener。

既有 JSON client/router 安全 profile 未添加新字段时行为保持；新增 `peer` / `federation` 配置见[部署拓扑](topology.md)。Rust 直接构造 profile 结构体的调用者需要补上 `peer: None` 或 `federation: None`。doctor 不再将 session open 当作独立的 TLS 握手证明。
