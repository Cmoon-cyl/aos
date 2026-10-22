# 参数快照加载

`load` 将保存的值应用到目标 Node 已声明的参数，整批成功或整批失败。
它不导入文件中的声明、descriptor、source、记录版本或 Node 身份，也不自动声明缺失的参数。
目标当前的类型、范围、只读标志和 `allowed_states` 始终有效。

Rust `ParameterStore::prepare_load(snapshot)`、`prepare_load_values(parameters)`、
Python `Node.load_parameters(path)` 和远程 `ParameterClient::load_atomically(...)`
共用这套规则。CLI 的文件解析仍由 CLI 负责，提交使用同一个远程原子加载入口。

| 文件中的值与目标状态 | load 结果 |
| --- | --- |
| `NotSet`，目标仍已声明且未初始化 | 保留声明、descriptor、source 和记录版本；动态、静态声明均适用 |
| `NotSet`，目标已初始化 | 整批返回 `conflict`，不删除或清空参数 |
| 只读参数，值相同 | 保留原记录 |
| 只读参数，值不同 | 整批返回 `read_only` |
| 普通参数，值相同 | 保留原记录 |
| 普通参数，值不同 | 按目标当前 descriptor 校验，通过后修改 |
| 名称未声明或批次中重复 | 整批失败 |
| 空加载批次 | 按无变更事务处理 |

所有输入项都进入候选批次，包括同值、只读和 `NotSet` 项。`allowed_states` 校验及应用
validator 不会因为某项无变更而跳过；validator 在锁外运行，提交时仍复查整批版本、
生命周期 epoch 和事务期限。因此，客户端不能先 describe 再过滤这些项。
快照的 `version` 是导出时的记录，不是目标 Node 的提交版本条件。

全批无变更时，仍运行 validator 并完成上述复查，但不增加存储或记录版本，不发布参数事件，
不触发 `use_sim_time` 的来源订阅或模式配置。返回的 `ParameterCommit.event` 含空的
new/changed/deleted 列表，`event_published=false`，版本为目标当前版本。
混合批次只为实际变化的参数增加一次全局版本、发布一个事件；未变化的记录保持原样。

普通 `set` 的行为保持原样：动态参数的显式 `NotSet` 表示删除，只读参数即使写入同值也拒绝，
静态参数不能用 `NotSet` 清空，空原子 set 仍拒绝。加载文件中的 `NotSet` 不承担删除意图。

## 快照文件与启动覆盖

`ParameterSnapshot::write` 和 Python `Node.dump_parameters` 使用 core 共用的
`write_private_atomic`：在目标目录写私有临时文件，同步文件后 rename 替换目标，再同步父目录。
Unix 新文件权限为 `0600`；已有普通文件的权限位保持不变。符号链接、目录和只读目标明确拒绝。
替换前失败保留原文件，并尽力清理本次临时文件；替换后发生其他同步错误时，错误文本明确标明
“destination already replaced; durability not confirmed”。

部分文件系统不支持目录同步。目录同步返回 `EINVAL` / `ENOTSUP` 时写入仍算成功，
但只能确认文件本身已经同步，不能确认目录项在崩溃后仍保留。
共用 helper 的 `AtomicWriteReport.directory_synced` 会返回 false；现有
`ParameterSnapshot::write` 和 Python `dump_parameters` 保持原返回类型，不新增自动持久化或断电零丢失承诺。

`startup_config()` 保留显式 `NotSet`。应用若用具体默认值声明静态参数，而启动配置对该参数给出
`NotSet`，声明返回 `wrong_type`，不再悄悄把默认值改为未初始化。应用仍可用具体类型加
`default=NotSet` 显式声明未初始化参数；也可显式 `ignore_override=true` 使用代码中的默认值。
动态声明的既有行为保持不变。

## 服务与版本兼容

六个参数服务的名称保持不变。仅 `~/set_parameters_atomically` 的请求增加可选字段：

```json
{
  "parameters": [
    {"name": "pending", "value": {"type": "not_set"}},
    {"name": "limit", "value": {"type": "integer", "value": 4}}
  ],
  "mode": "load"
}
```

`mode` 只接受 `"set"` 或 `"load"`；省略时为 `"set"`。逐项
`~/set_parameters` 不接受该字段。Rust 的 `set_atomically(...)` 继续发送原有形状，
`load_atomically(...)` 发送完整候选和 `mode: "load"`。

旧形状 JSON 的默认语义保持不变，**旧编译二进制并不因此兼容**。AOS 使用 Schema 的哈希路由，
可选字段也会改变接口哈希，SDK 和 CLI 必须同批重建更新。
服务 `type_name` 仍为 `aos_interfaces/srv/SetParametersAtomically`。

| 接口 | 此次修复前哈希 | 此次修复后哈希 |
| --- | --- | --- |
| SetParametersAtomically | `8abee1265429b75d5c959d2d8504738552476438c4502661bafbf714bf984d03` | `b04ae3615764725db87d3af97c119f46c5fbe626c421c2fe4a7072c801d2a1a7` |

DescribeParameters、GetParameterTypes、GetParameters、ListParameters、SetParameters
五个接口的哈希未变。回归测试固定了六个当前哈希及原 atomic 请求的哈希。

## `/clock` 来源缓存

首次尝试启用 `use_sim_time` 时，Node 在最终提交前准备一份 `/clock` 订阅。
若准备期间生命周期 epoch 改变，事务会返回 `conflict`，参数值、版本和时钟模式保持原样；
已准备的来源仍作为 Node 所有的缓存保留。关闭模拟时间也保留该缓存，后续启用复用它，
每个 Node 至多一份，并在 Node 销毁时关闭。

这项资源分配不代表参数已经提交。无变更 load 不准备来源；有变更但失败的启用尝试可能
留下上述缓存。Memory transport 回归覆盖创建期间发生 epoch 冲突、缓存复用和销毁回收。
