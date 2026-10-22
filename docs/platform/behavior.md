# AOS 0.2 行为契约

本文说明名称、配置、QoS、时间、进程与资源管理的行为约定。具体命令字段以 `aos schema GROUP VERB` 为准；使用入口见[平台文档](README.md)。

## 责任和兼容边界

| 层 | 必须负责 | 不得接管 |
|---|---|---|
| Rust core | 名称、Interface 校验、协议状态、参数和生命周期公共规则、关闭与等待 | Agent 模型、审批、业务持久化 |
| Zenoh adapter | 实际 key、QoS 映射、传输、Graph、认证配置和可观测丢样 | 从 Node 名或 payload 自述推导可信用户身份 |
| Python SDK | Node/Timer/Executor API、业务回调、参数/lifecycle 回调 | 另建 Action 状态机；在传输回调中执行阻塞业务 |
| CLI | JSON 解析、自描述、有限观察、重复调度、hook 与诊断 | 用等待超时推导业务未执行；用取消受理推导 CANCELED |
| launch/supervisor | 本机进程、依赖就绪、退出、重启、整组关闭 | 恢复旧进程内的 goal；自动重试有副作用调用 |
| OS 服务管理器 | 开机启动、退出登录后保活、supervisor 自身恢复 | 与 supervisor 同时重启同一业务进程 |

新增入口沿用 JSON 请求、查询命令 JSON 输出、流命令 JSONL 输出；日志写 stderr。错误至少包含稳定 code、message、command、阶段和已知 endpoint/request_id/goal_id。尚未发送的参数错误与发送后的等待失败必须可区分。兼容性与安全拒绝没有足够远端证据时，不能把普通 timeout 改报为“远端拒绝”。

## 名称与配置

覆盖顺序从低到高：

| 层 | 含义 |
|---|---|
| 1 库默认 | 版本化的默认值 |
| 2 程序初值 | NodeOptions 中允许部署覆盖的值；不构成锁定 |
| 3 配置文件 | 按显式文件列表从前到后覆盖；文件内重复键报错 |
| 4 环境变量 | 仅公开白名单；不把任意环境变量展开成配置，也不执行文本 |
| 5 launch Node 配置 | launch 为该进程/Node 产生的显式部署值 |
| 6 本次 CLI 显式覆盖 | `run/launch` 或独立命令的调用参数；同一来源对同一标量重复赋值报错 |

覆盖只作用于已提供的字段，未提供值不抹掉低层值；`null` 只用于 Schema 明确允许清空的字段。配置对象递归按键覆盖，普通数组整体替换，remap 规则按下面的专用规则处理。未知键、非法枚举、布尔值冒充数字、NaN、Infinity、溢出均在建实体前拒绝。凭证只显示配置来源及路径，不在 `effective_config` 输出私钥或密码。

每个生效值提供 `value/source`；端点同时提供 `original_name/resolved_name`，QoS 提供完整值和来源。CLI、SDK、Graph、NodeCard 使用同一个 core 结果。修改已有 Node 的网络身份、name、namespace、端点或 QoS，须重建对应实体；参数服务不隐式执行这种重建。

名称展开沿用现有相对名、绝对名、`~/` 私有名及 `{node}/{namespace}/{ns}` 规则。AOS 提供**精确名称映射**：规则包含可选 Node selector、`kind=topic|service|action|any`、`from/to`。本期不实现通配符、正则、反向引用和任意替换变量；出现这些语法明确拒绝。

1. 先按声明时 Node 名选择 Node 专属配置，固定该选择结果。
2. 应用最终 Node 名，再应用 namespace；以最终身份展开私有和相对端点名。
3. 高优先级来源先匹配；同来源 Node 专属规则先于通用规则，同组按列表顺序取第一个精确匹配。相同 selector/kind/展开后 from 出现两个不同目标报错，不能靠排序掩盖冲突。
4. 每个名称最多映射一次；`a→b` 与 `b→c` 不把 `a` 连续改成 `c`。
5. 先映射 Action 整体名，再生成 `/_action/send_goal|get_result|cancel_goal|feedback|status`。拒绝单独 remap 内部五个实体，防止一个 Action 被拆散。
6. 完成名称校验和映射后，adapter 才生成 key；ACL 按**实际 key**匹配。Graph、NodeCard 只发布最后的身份和端点，并保留来源诊断。

例：Node 初值 `name=worker, namespace=/a`，部署改为 `worker_2,/b`，私有 `~/status` 展开为 `/b/worker_2/status`。Action `/a/job→/b/job` 后五个内部实体全部位于 `/b/job/_action/`。

与 ROS 2 的对应和简化见[固定来源表](sources.md)：保留 Node 名→namespace→端点顺序、局部优先和首次匹配；不宣称支持 ROS remap 的全部语法。

## QoS

所有 profile 先完全展开为四个显式字段；`preset` 后提供的字段覆盖预设。`history=keep_last`、正整数 `depth` 是唯一支持的 history 组合。

| profile / 使用处 | depth | reliability | durability | 本期允许覆盖 |
|---|---:|---|---|---|
| `default`：普通 Topic、Action feedback | 10 | reliable | volatile | depth、两种 reliability、两种 durability |
| `sensor_data`：用户显式选择的 Topic | 5 | best_effort | volatile | 同上 |
| `services_default`：Service client/server、Action send_goal/get_result/cancel_goal | 10 | reliable | volatile | depth；非 reliable/volatile 明确拒绝 |
| `action_status`：Action status | 1 | reliable | transient_local | depth、两种 reliability、两种 durability；改变值会改变晚加入保证 |
| `parameters`：参数服务 | 1000 | reliable | volatile | depth；非 reliable/volatile 明确拒绝 |
| `parameter_events`：参数事件 Topic | 1000 | reliable | volatile | Topic 支持字段；晚加入读当前参数使用 get |
| `lifecycle_services`：lifecycle 服务 | 10 | reliable | volatile | depth；非 reliable/volatile 明确拒绝 |
| `lifecycle_events`：`~/transition_event` | 10 | reliable | volatile | Topic 支持字段；当前状态用 get_state |
| `clock`：模拟/回放时钟 Topic | 1 | best_effort | volatile | Topic 支持字段；同一 namespace 仅一个有效时间源 |

这张表采用固定 Jazzy 的已有预设：该快照的 `parameter_events` 是 **volatile**，不要凭其他发行版或文档印象改成 transient_local。Action server/client 分别接受 `goal_service/result_service/cancel_service/feedback/status` 五个 profile；不能用一个模糊 `qos` 覆盖五个通道。完整 JSON 字段由相应命令的 `schema` 输出给出。

以下项目不在本期：`keep_all`、`deadline`、`lifespan`、`liveliness`、`liveliness_lease_duration`、DDS 的 `system_default/unknown/best_available` 策略值。公开入口必须返回 `UnsupportedQoS` 或等价稳定错误，含字段和值；不能接收后静默忽略。`preset=default` 是确定的 AOS 预设，不等于 DDS 的 `SYSTEM_DEFAULT`。

实际 adapter 义务：

- Topic：depth 约束本地 KEEP_LAST 队列；transient_local 的发布缓存也使用该 depth。reliable/best_effort 必须接入固定 Zenoh 可用的 reliability/拥塞配置，并记录实际映射；不能只把值写入 Graph。
- Service：server depth 约束待处理请求，client depth 约束 client 生命周期内待消费响应。保留每个请求第一条有效 reply 的关联规则；队列满时沿用 KEEP_LAST 丢弃最旧项并报告 drop，受影响的本地等待者要明确结束。它不限制全部执行中请求，也不保证请求一定执行。执行中请求上限是单独的资源限制。基线每次 query 只收第一条 reply，单纯增大该一次性队列不会使 client depth 有意义；共享接收队列完成前，非默认 client depth 必须返回 Unsupported。
- Zenoh 1.10.1 的 reply 继承 query 拥塞控制，reply builder 的独立设置无效果；本期不接受会被忽略的 Service best_effort。后续若扩展请求拥塞选项，须单列能力和测试，不能声称等于双端 DDS QoS。
- volatile 不保留用于晚加入的历史；transient_local 只在发布实体仍存在且缓存未过期/淘汰时提供历史，不能等同持久数据库。晚加入 Action status 要用真实 Action 验证，通用 Topic 测试不能替代。
- 重传/可靠传输与业务去重、幂等、恰好一次执行分开；重连不重发 Service 或 goal。

兼容性诊断至少输出 requested/offered 的完整值、实体 ID、`compatible|incompatible|unknown`、原因及 `enforcement`。按请求/提供模型，best_effort 发布与 reliable 订阅、volatile 发布与 transient_local 订阅应报告不匹配；depth 差异本身不阻止匹配。这只是诊断规则：Zenoh 上是否阻断传输须单列实测，不能把 Graph 比较结果写成 DDS 匹配保证。未知/旧版本未发布的 QoS 不能猜成默认或 compatible。

## 时间、Timer 与 Rate

| 时间域 | 用途 | 行为 |
|---|---|---|
| `steady` | 超时、重启退避、资源清理、记录间隔、关闭和 hook 执行上限 | 单调、不随系统校时或 bag 暂停改变 |
| `system` | 可交换的墙钟时间戳、日志 | 允许跳变；跨机 delay 必须另有同步证据 |
| `ros` | Node 业务时间，模拟与回放 | `use_sim_time=false` 时取系统时间；启用后由指定 clock Topic 驱动 |

`Node.clock` 默认是 `ros`、未启用模拟时间；Timer/Rate 未指定 clock 时使用 Node.clock，也可显式选择 steady/system。所有控制等待始终使用 steady，不能由 `use_sim_time` 改写。秒使用有限非负数，转换时检查溢出；`null` 仅在公开支持无限等待的字段表示无应用期限，`0` 不统一解释为无限。

启用模拟时间但尚未收到时钟时，时间状态为 `uninitialized`，业务 Timer/Rate 不推进；收到值为 0 的首条时钟也记录“已接收”，不能把合法 0 与未初始化混为一谈。暂停时消息可以继续报告相同时间，Timer 不触发新周期。切换时间源、向前/向后跳变均有事件，包含前后值、时钟域和原因。连续向前更新合并为一条前后区间记录，并单独累计更新数；初始化、回退和源切换分隔区间，避免高频 clock 样本覆盖这些诊断。

Timer 沿 rcl 的单次调度、错过周期合并和时间跳变机制实现。向前更新只唤醒等待者，不提前消耗正在执行回调之后的周期；慢回调结束后允许再领取一次逾期回调，领取时推进到下个有效周期，不一次补跑所有漏掉的业务。向后跳到上次调用之前时，下次触发为新时间加一个周期。其余回退和切换时间源保留/重算剩余周期，按固定 rcl 用例核对；回放 seek 属于显式跳变。每个 Timer 同时至多一个回调，业务在 Executor 执行；close/shutdown 唤醒 Rate 和等待者，不遗留 Timer。

pub rate 是调度目标，不是 QoS 或硬实时承诺。hz 按本地 steady 接收间隔计算；bw 报告序列化业务 payload 字节数，不冒充网卡流量；delay 只在源时间戳可用、来源/时间域可识别且时钟可比较时计算。否则返回 `unavailable` 与原因。

## 发布、观察和 hook

`Publisher.publish()` 始终只提交一条消息；CLI 才负责重复调度。

| 操作 | 固定行为 |
|---|---|
| pub 默认 | 一次；不等待订阅者；结果表示提交成功，不表示消费者处理完成 |
| pub 模式 | `once=true`、`times=N`、`follow=true` 三选一；都未提供时一次。`once=false` 单独使用无效 |
| pub rate | 默认 1 Hz，只控制重复发送；第一条立即发送，之后按 steady 调度；漏周期不突发补发 |
| 等订阅者 | `wait_for_subscribers=0` 默认关闭；正整数 N 配合 `match_timeout`。匹配数与类型、QoS 的计数依据必须在输出注明 |
| pub 退出 | 记录 submitted_count、stop_reason；可选 `linger` 仅延长实体存活，不是消费确认；不得宣称本期有业务 flush/ack |
| echo 默认 | 订阅建立后观察 30 秒；`follow=true` 且未显式给 timeout 才无限观察；once/count 仍可终止 |
| echo 等待 | discovery_timeout 与订阅后的 timeout 分开；idle_timeout 自订阅完成起，按每条有效接收消息重置（包括被过滤的消息） |
| echo 计数 | received 为有效接收数；filtered 为通过过滤数；count/once 对 filtered 计数；输出保留两者 |
| Service 重复调用 | 默认一次；`times/interval` 是显式的多次调用，每次独立 request_id 和结果；上一条结束再发下一条，失败默认结束，不自动重试 |

pub 的 `discovery_timeout` 用于类型发现；旧 `timeout` 保留为 discovery/match 两阶段上限的兼容别名，但不再隐式开启订阅者等待。`timeout` 与任一新分阶段字段同时提供时返回冲突错误；未给上限时各阶段默认 30 秒。

观察顺序为“完整接收 → 过滤 → hook → 输出字段投影 → 判断停止”。字段投影不改变 hook 输入。过滤 DSL 使用字段路径、存在性、严格类型比较及 all/any，不执行任意表达式；字段不存在在 `exists` 中是 false，在其他比较中按未匹配处理并累计诊断，不静默转成 0 或空字符串。

同一观察由第一个提交的终止事件结束。若同一条消息同时满足 hook 和 count，报告 hook_matched；已到截止时间的消息不再送入 hook。总时限包含 hook 执行，不能被慢脚本延长。终止输出含 stop_reason、计数、耗时；hook 成功包含命中的完整消息和投影。停止观察不停止 provider。

两种 hook：

- 字段条件：`path/op/value` 或 all/any 组合；`consecutive_matches` 默认 1，未匹配消息重置连续计数。
- 本地脚本：用户显式给 executable 和 argv，不经过 shell；不得由 NodeCard 或消息内容产生执行命令。每条通过过滤的消息启动一次受控进程，stdin 为一条 JSON，含完整 message、topic、sequence 和接收时间域；stdout 只允许一个 JSON 对象，`decision` 为 `continue|matched|error`，可附 reason。

脚本默认 1 秒超时、stdout 上限 64 KiB；非零退出、无效 JSON、超时、超限均终止观察并报告 hook_error。等待和执行位于观察 worker，不在传输回调；队列有界，溢出要报告 hook_overflow，不能默默跳过可能命中的消息。中断/关闭先终止该次脚本进程组、回收后输出最终事件；不杀 provider。任一返回 `error` 不被当成 `matched`。

## 参数、lifecycle 和进程

参数按 Node 所有，默认必须先声明。类型、默认值、说明、范围、只读标志及修改状态限制可查询。启动覆盖仅改变声明的初始值，不跳过类型和校验。`set_parameters_atomically` 在全部校验通过后一次提交；失败不部分写入、不发成功事件。校验回调不得承担业务副作用；参数提交成功不代表硬件/外部任务已完成。dump/load 显式读写文件，平台不暗中把每次 set 持久化。

load 保留已声明且仍未初始化的 `NotSet`、接受只读同值，并让所有项参加校验；全批无变更时不增加版本或发布事件。远程加载通过 atomic 请求的 `mode: "load"` 表达，不改变普通 set 的删除语义。具体规则及接口哈希迁移见[参数快照加载](parameter-load.md)。

`allowed_states` 的空数组表示不限制；非空数组只接受四个 Lifecycle 主状态，普通 Node 声明时返回 `unavailable`。限制适用于后续 set/delete/load，声明和启动覆盖仍可安装初值。prepare 在当前状态不允许时返回 `state_not_allowed`；validator 期间状态 epoch 改变则 commit 返回 `conflict`，即使已经回到同名状态。完整核心契约见 [Lifecycle 与参数修改状态限制](lifecycle-core.md)。

LifecycleNode 保留 ROS 主状态和转换回调：unconfigured、inactive、active、finalized 及 configuring/activating/deactivating/cleaningup/shuttingdown/errorprocessing。查询、参数和生命周期控制通道保持可用。普通 Node 不强制受管。

| 受管实体 | inactive 时 | deactivate/shutdown 时 |
|---|---|---|
| publisher、Timer | 不发送、不调度业务；显式返回 inactive 或可观察跳过 | 立即阻止新业务；关闭等待有界 |
| subscription | 不派发业务回调；队列按 QoS 有界，不追补停用期间业务 | 已入执行器的回调按 drain 处理 |
| Service | 新业务请求明确返回 inactive 错误 | 停止新接收，已执行请求允许在 drain 期限内完成 |
| Action | 新 goal 拒绝；status/get_result/cancel 控制通道保持响应 | 已有 goal 由 ActionServer/业务回调决定结果，不能合成 CANCELED |

每次转换串行执行，回调 failure/error 按 ROS 转换表落到可查询状态并发事件。默认 drain 5 秒；到期报告仍在途对象，deactivate 转换失败而非假报全部结束。强制进程退出只报告连接/进程消失，不合成业务终态。各实体的准入和在途处理由同一生命周期状态控制。

run 从安装包资源索引查入口和 Schema，不依赖 cwd；重复包/入口歧义报错。launch 配置是声明式数据，不在解析配置时运行任意代码。依赖等待以显式 readiness/lifecycle 条件为准，process_started 与 discovered/active/ready 分别报告。

supervisor 默认不重启；显式 `on_failure` 时默认延迟 1 秒、最多 3 次，用户可配置有限上限。人为整组停止不触发重启。先停止依赖方，再停止 provider；默认给 SIGINT 5 秒、SIGTERM 5 秒后 SIGKILL，记录每步与残留检查。这些数值是 AOS 默认，ROS launch 的无限 respawn 不直接搬入。常驻部署由主机的 OS 服务管理器负责，见[系统服务](system-service.md)。
