# Agent 的网络发现与热更新

`graph snapshot` 把已发现的 Node、服务归属、接口类型和副作用声明放在一次查询里；
`graph watch` 持续维护同一份发现视图；`graph wait` 在期限内等待指定节点或能力出现。
三者复用 Rust core 的持久 Graph watch，不需要额外发现服务。

这里的网络范围是**当前配置已连通、同一 AOS domain、发现权限可见的网络**。
每台机器的 Router 连接按[部署指南](deployment.md)配置。发现不会扫描 IP、自动打通 NAT，
也不会把未连接的机器加入网络。空结果只表示本次观察没有发现匹配项。

## 一次拿到可选能力

```sh
aos schema graph snapshot
aos graph snapshot
```

返回 `ok/data` JSON。`data.nodes` 中每个节点包含：

| 字段 | 用途 |
| --- | --- |
| `node_id`、`name` | 实例身份与完整名称；重启后 ID 改变 |
| `introspection` | NodeCard 读取状态、错误码和最近尝试时间 |
| `display_name`、`summary`、`tags` | 帮助选择节点的远端说明 |
| `lifecycle`、`parameter_version` | 观察运行状态和配置版本变化；不输出参数值 |
| `endpoints` | 当前端点的 ID、种类、名称、类型/hash、说明、副作用与契约版本 |

默认不展开完整 Interface schema，也不输出参数值和进程环境。隐藏路径、已识别的内部
控制接口、Client 和 ActionClient 默认不列出，诊断时可显式开启。
根命名空间中符合 `/aos_cli_<32 位十六进制>` 的临时 CLI 节点也默认隐藏，避免每次调用
产生无关变化；`include_cli_nodes:true` 或精确 `node_name` 选择可查看。这只是命名约定，
不能作为身份认证。

例如只查归属于 `/fleet` 下节点的 Service，把以下内容保存为 `discovery.json`：

```json
{"namespace":"/fleet","kind":"service","timeout":5}
```

```sh
aos graph snapshot --request discovery.json
```

`namespace` 按节点的命名空间和路径边界选择子树，不按端点名称选择；`node_name` 和 `endpoint_name` 按完整名称精确匹配。
不会把 `/fleet2` 算进 `/fleet`。默认观察窗口为 0.5 秒，整个查询默认最多 5 秒；
NodeCard 最多八个并发读取，每个默认最多 0.5 秒，并受剩余总时限约束。
无响应的节点会留下 `unavailable` 记录，不会阻止其他节点进入视图。

## 判断能否使用

发现包含不同强度的证据：

| 观察 | 能说明什么 |
| --- | --- |
| Graph 中出现节点或端点 | 收到了该实例的存活声明 |
| `introspection.status=ok` | NodeCard 在期限内返回，ID 与当前观察一致 |
| 业务调用的响应 | 这一次操作的结果；仍需判断其业务成功条件 |

前两项不证明业务健康、调用权限或认证身份。不能为了探测“可用”而执行任意业务服务。
未配置的 `side_effects` 保持 `null`；不会默认为只读。生命周期 gate 关闭时，即使说明
读取成功也不能当作业务就绪。

`pending` 表示说明尚未取得，`unavailable` 表示读取失败，`disabled` 表示请求只看发现。
只有端点而没有 Node 声明的实例标为 `undeclared`，保留端点但不查询说明，也不为此
耗尽快照时限。`ambiguous` 表示同名 Node 存在多个实例，此时停止按名称读取说明。
NodeCard 与已发现 ID 不符时报告 `IDENTITY_MISMATCH`，不会把旧实例的描述挂到新实例上。

Service 和 ActionServer 的 `provider_selection` 会指出同名服务的多个提供者。
这一检查覆盖观察到的整个 Graph；只过滤某个节点不会消除调用名称的冲突。
Agent 应先处理冲突，再读取所选 Interface 并按实际任务授权调用。

## 热发现与缓存合并

把以下内容保存为 `watch.json`，观察 30 秒：

```json
{"timeout":30,"refresh_interval":2}
```

```sh
aos graph watch --request watch.json
```

输出 JSONL，stdout 与 stderr 要分别读取：

1. `snapshot` 是完整初始视图，`sequence=0`。此时有些 NodeCard 仍为 `pending`。
2. `delta` 包含 `added`、`updated` 和 `removed_node_ids`。按 `node_id` 删除或替换完整记录。
3. `summary` 表示本地观察结束；失败时还会输出 `error`，应同时检查退出码。

同一 `watch_id` 内序号连续；新观察总是发送新基线。出现错误、序号缺口，或在没有结束
信息时遇到 EOF，应重新开启观察并替换缓存，不能继续套用旧序号。

Node 加入、退出以及端点增删由同一个持续发现订阅触发。说明定期重读，用于发现
生命周期、参数版本等不改变 Graph 声明的更新。端点集合变化时立即失效旧描述，再查询新
描述；过期 NodeCard 不会复活已移除的服务。成功重读但内容没有变化时不输出 delta。
重读失败时，同一 Graph 的上次成功说明会保留，标为 `introspection.stale:true`，
端点标为 `description_status:stale`，并保留 `last_success_at_unix_ms`。这些内容不代表
当前状态，不能满足要求说明的 wait。Graph 改变或身份不符时不沿用旧说明。

`refresh_interval` 是最短重试间隔，大量节点或慢节点可能让实际刷新更晚。默认 5 秒。
`checked_at_unix_ms` 仅是最近一次尝试时间，时间戳变化本身不触发事件。
发现通知允许合并：该接口保证当前视图收敛，不是每次短暂变化都可重放的事件日志。
移除也只表示“不再出现在当前过滤视图”，无法单凭它区分关机、断网、权限变化或端点变化。

只有显式 `{"follow":true}` 且省略 `timeout` 才无限观察。SIGINT/SIGTERM 停止本地观察。
输出队列有界，慢消费者导致失败时要重新获取基线。安静的无限观察可能要到下次写入
才发现读取端已关闭，因此宿主应主动终止自己启动的观察进程，或使用有限 timeout。
空闲时仍检查信号和输出错误，但只有 Graph 通知或到期的说明刷新才重新构建视图。Skill 提供操作流程，不会自行建立
后台订阅，也不会让未运行的 Agent 自动醒来；这需要宿主自己的任务调度或事件接入。

## 等待指定能力

把以下 JSON 保存为 `wait.json`：

```json
{"endpoint_name":"/fleet/inspect","kind":"service","timeout":20}
```

```sh
aos graph wait --request wait.json
```

默认要求 NodeCard 读取成功且端点描述匹配；它等待的是发现与说明证据，不执行服务，
也不等待业务 gate 自动打开。`nodes` 保留全部选中节点；`matched_node_ids` 列出真正
满足证据要求的节点，`selected_not_matched` 统计其余节点。不要把选中等同于已匹配。
同一节点的同名同类型端点若声明互相冲突，描述保持 `unknown`；要求描述的等待不会
替 Agent 选择其中一个。
只等广播出现时，显式设置 `require_descriptions:false`。

超时返回退出码 4、错误码 `WAIT_TIMEOUT`，并在 `error.last_observation` 提供最后一次视图。
Agent 可以据此判断还在等待、说明不可读，还是存在同名冲突。

默认最多 256 个节点和 4096 个端点，可通过请求提高到 4096 和 32768；投影视图另有
2 MiB 上限。超限明确失败，不静默丢节点。可按命名空间分开观察；这些独立查询不构成
同一时刻的全网原子快照。说明文字裁剪通过 `description_truncated` 标记，范围包括 NodeCard 中被视图过滤的端点。
状态和版本字段严格保留小型标量；不合类型的值丢弃，并用 `metadata_sanitized` 标记。
2 MiB 限制可能先于节点数上限达到。
