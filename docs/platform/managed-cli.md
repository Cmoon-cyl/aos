# 参数与生命周期管理

先设置 `AOS_ZENOH_CONFIG` 和 `AOS_DOMAIN_ID`，运行目标版本的
`aos schema GROUP VERB` 查看完整输入。所有命令通过 `--request PATH|-`
读取 JSON；`node_name` 是目标 Node 的绝对名称，例如 `/demo/worker`。
CLI 创建独立的本地 Node，使用 core 的 ParameterClient、LifecycleClient
调用目标服务。参数值、事务和生命周期状态均由 provider 的 Rust core 管理。

## 查询和原子修改参数

| 命令 | 主要输入 | 输出 |
| --- | --- | --- |
| `param get` | `names`：非空、无重复的参数名数组 | 同一版本的名称和带类型值 |
| `param describe` | `names` | 默认值、当前值、约束、来源、版本 |
| `param list` | `prefixes:[]`、`depth:0` | 名称和前缀；`depth:0` 表示不限制深度 |
| `param set` | `parameters`：非空对象 | 一次原子提交的结果 |
| `param dump` | `format:"startup"` 或 `"snapshot"`；可选 `path` | 导出的 JSON；指定路径时也写入文件 |
| `param load` | `path` | 一次原子提交的结果 |

`depth` 和数值 transition ID 必须使用 JSON 整数表示，例如 `3`。
`3.0`、`3e0` 不会被默认为其他值，而是在打开 Context 前报
`INVALID_ARGUMENT`。

```sh
aos param set --request - <<'JSON'
{
  "node_name":"/demo/worker",
  "parameters":{
    "speed":3,
    "labels":{"type":"string_array","value":[]}
  },
  "timeout":5
}
JSON
```

标量和非空同类型数组可以直接写值。空数组必须使用 `{type,value}`；
字节数组也要明确写 `byte_array`。整数、浮点数和布尔值保持各自类型。
CLI 复用 core 的解析器，拒绝混合数组、超范围整数和未知类型字段。

`set` 和 `load` 始终调用 `set_parameters_atomically` 一次。任意值违反
类型、范围、只读、生命周期 `allowed_states` 或用户 validator 约束时，
整批不提交。远端拒绝返回 `PARAMETER_*` 错误码及 provider 的具体结果。
`set` 传入 `null` 或 `{"type":"not_set"}` 时按 core 的参数删除规则处理；
`load` 使用下述恢复语义，不把未初始化值当作删除请求。
CLI 不会自动声明缺失参数，也不会覆盖 provider 的 descriptor。

成功结果中的 `event_published:false` 只说明参数事件未成功发布；
应根据 `successful`、`version` 和 `transaction_id` 判断提交结果。

## 导出、加载和启动恢复

```sh
aos param dump --request - <<'JSON'
{"node_name":"/demo/worker","format":"startup","path":"worker-parameters.json"}
JSON
```

默认的 `startup` 文件是纯 JSON：

```json
{
  "parameters":{
    "speed":{"type":"integer","value":3},
    "labels":{"type":"string_array","value":[]}
  }
}
```

它可放入 `Node(..., config_files=[...])` 的启动配置，也可交给
`param load`。启动恢复在应用声明参数时应用并验证覆盖值；读取配置本身
不会创建参数。在线 `load` 则验证现有参数的当前约束。

启动文件中的 NotSet 可以保留明确声明为未初始化的参数；如果新程序以具体
默认值声明静态参数，旧文件的 NotSet 覆盖会被拒绝，不会静默移除该默认值。

在线 `load` 使用原子服务的 `mode:"load"`：未初始化值只在目标仍未初始化
时保留其声明，目标已初始化时报告冲突；只读参数的相同值保留，改值仍拒绝。
全部候选仍一起接受 allowed_states、validator 和版本冲突检查。同值项不改
记录版本，不发布变化事件；空 load 是有效的无操作。`param set` 保持原有
行为，显式 `null` 可删除动态参数，空批次拒绝。

这次修复改变 `set_parameters_atomically` 的接口 hash，SDK 与 CLI 需要
同批更新；不能将旧二进制与新 provider 混用。其他五个参数服务的 hash
保持不变。

`format:"snapshot"` 保存 core 版本 1 的快照格式，包含 Node 身份、
版本、descriptor 和来源。加载快照只取其中的名称和值，不用旧文件中的
descriptor、来源或版本替换 provider 的状态。

导出先列名称，再描述全部参数。两次回复的版本不一致，或列出的参数在
描述前被删除时，CLI 返回
`PARAMETER_CONFLICT`，不写文件、不自动重试。版本相等表示同一 provider
参数存储中没有夹入修改；Graph 身份和 RPC 回复均未经认证，不能据此证明
跨重启、同名替换或不可信网络中的全局快照一致性。

加载文件在打开 Context 前读取并解析一次，之后把已解析的值交给远端。
文件只能是上述启动格式或版本 1 快照；不接受其他 Node 配置字段。
导出路径会先检查父目录和目标类型；实际写入使用同目录临时文件再替换。
Unix 上新文件权限为 `0600`，替换已有文件时保留其普通权限位；
替换前同步文件内容，替换后同步父目录。
结果中的 `file_write.directory_synced` 表示父目录是否同步成功；文件系统
不支持目录同步时为 `false`，文件已替换，但不能确认目录项的崩溃持久性。
没有指定输出路径时 `file_write` 为 `null`。替换后的其他同步错误仍返回
失败，并明确说明目标文件已经替换。
文件读写失败返回 `FILE_IO_ERROR`。若远端查询已成功但本地写入失败，
错误会说明该阶段，不把它描述为远端修改失败。

## 查询和切换生命周期

| 命令 | 主要输入 | 对应标准服务 |
| --- | --- | --- |
| `lifecycle get` | `node_name` | `get_state` |
| `lifecycle list` | `view:"transitions"`，默认值 | `get_available_transitions` |
| `lifecycle list` | `view:"states"` | `get_available_states` |
| `lifecycle list` | `view:"graph"` | `get_transition_graph` |
| `lifecycle set` | `transition`；可选 `drain_timeout` | `change_state` |

```sh
aos lifecycle set --request - <<'JSON'
{"node_name":"/demo/worker","transition":"deactivate","drain_timeout":2,"timeout":5}
JSON
```

transition 可用公开 ID `1`–`7`，也可用 `configure`、`cleanup`、`activate`、
`deactivate`、`shutdown`。`shutdown` 由 provider 根据当前状态选择对应 ID。
CLI 不维护另一份状态机，也不替 provider 选择结果边。

`drain_timeout` 的单位是秒；省略时使用 core 的 5 秒默认值，`0` 表示不等待
已经受理的业务工作。它限制回调返回后的 drain 等待，不能终止同步 Python
回调。转换拒绝或失败保留完整 provider report；drain 超时使用
`LIFECYCLE_DRAIN_TIMEOUT`，并返回剩余工作。进入 `finalized` 不等于销毁
Node 或结束进程，资源清理由应用显式执行。

## 等待、错误与中断

`timeout` 默认为 30 秒。一次命令在 Context 创建后固定一个 steady deadline；
Node 发现、所需标准服务发现及全部 RPC 共用剩余预算。`list`、`dump`
不会为每次查询重新计算完整超时。文件预检发生在网络操作之前；Context
建立及本地文件系统操作不属于远端查询预算。

超时返回退出码 `4`，SIGINT 返回 `130`，业务拒绝和文件错误返回 `1`，
输入错误返回 `2`。SIGINT 关闭本次 CLI 的本地 Context，使本地等待结束。
已提交的参数事务或生命周期转换可能继续执行；超时、中断都不表示远端
回滚或停止。任何失败均不会触发自动重试。可随后显式查询参数值、
`lifecycle get` 或 provider 的 `last_report` 判断最终状态。

每条命令只输出一份 JSON 结果。结果中的 `query` 记录预算和实际等待时长；
Graph 的 `node_id` 是发现到的身份，不能当作认证结论。
