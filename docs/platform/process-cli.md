# 已安装包与本机进程 CLI

`package` 查询安装索引；`run` 运行一个已安装入口；`launch` 按就绪条件启动依赖进程。三组命令都通过 `--request PATH|-` 接收严格 JSON。查看完整字段和输出协议：

```sh
aos schema package list
aos schema package info
aos schema package resource
aos schema run start
aos schema launch start
```

`aos run --request ...` 等价于 `aos run start --request ...`；`launch` 同理。查询返回一个 JSON 响应，运行命令返回 JSONL。输入不执行 shell、Python 模块或 launch 脚本。

## 找到已安装的包

安装布局、manifest 和运行层责任见 [launch.md](launch.md)。例如：

```json
{"prefixes":["/opt/aos"],"package":"demo"}
```

将上述请求交给 `aos package info --request request.json`，可获得包版本、安装前缀和 manifest 路径。其他查询如下：

| 命令 | 请求 | 返回 |
|---|---|---|
| `package list` | `{"prefixes":["/opt/aos"]}` | canonical 前缀和全部包候选，保留同名歧义 |
| `package info` | `{"package":"demo","package_version":"1.0.0"}` | 唯一匹配的包及 manifest |
| `package resource` | `{"package":"demo","resource":"pair"}` | 声明资源的类型和绝对路径 |

给出 `prefixes` 时，仅查询这份列表；空列表表示不查询任何前缀。未给出时，先读取 `AOS_PREFIX_PATH`，再加入已安装 `<prefix>/bin/aos` 自身的前缀。完整空字符串等同于未设置环境变量，非空列表内的空项会被拒绝。部署进程存在 `AOS_PROCESS_CONFIG` 时，仅使用 supervisor 固定的环境前缀，不再补入 CLI 的安装前缀。这些规则与 SDK 共用同一个解析函数。CLI 不把调用目录或源码仓库当作安装位置，开发目录中的 `target/release/aos` 不贡献隐式前缀。

同名包出现在多个前缀时，`info`、`resource`、`run` 和 `launch` 返回 `AMBIGUOUS_PACKAGE`；不自动选择最高版本。用显式前缀列表消歧。`package_version` 是精确版本校验，不会从多个候选中挑选一个。

索引查询不打开网络 Context。manifest、入口和资源均由 `aos-launch::PackageIndex` 校验；路径不能越出其安装前缀。

## 运行一个入口

```json
{
  "prefixes":["/opt/aos"],
  "package":"demo",
  "entry":"worker",
  "args":["one argument with spaces"],
  "shutdown":{"sigint_timeout":2,"sigterm_timeout":2}
}
```

交给 `aos run --request request.json`。默认进程 ID 为 `run`，默认 cwd 为包的 `share/<package>`，`args` 保持为逐项 argv。入口、cwd、配置文件和请求均在启动前校验。

`run` 可使用 `id`、`package_version`、`env`、`cwd`、`config_files`、`node`、`readiness`、`restart`、`context`、`shutdown` 和 `overrides`；确切形状以 schema 为准。不提供 readiness 时，运行器不创建控制 Context，也不会发出 `ready` 事件。程序正常退出只证明该进程完成，不能据此判断业务服务可用。

运行器保留 `AOS_PROCESS_CONFIG` 和 `AOS_PREFIX_PATH`。child 的 `env` 不得覆盖这些变量。`aos.process.v1` 要求 SDK Node 运行在受管 leader 的 PID 内；shell 包装入口必须用 `exec` 启动 SDK 程序。若代次身份相同而 NodeCard 的 PID 不同，就绪探针返回明确的 `READINESS_FAILED`。其他部署字段进入各自的配置来源；启动描述的生成、标准 SDK 消费和真实 NodeCard 结果需要分别验证。

部署入口自身也可以调用普通 CLI。`topic/service/action/node/interface/param/lifecycle/doctor` 和离线 `config resolve` 在配置阶段读取一次 `AOS_PROCESS_CONFIG`，将其中的 Context 和 Node 层合入请求；空值、相对路径、缺失文件或非法文档以 exit 2 返回，发生在打开网络之前。请求的 `context_config.launch|cli` 或 `node_config.launch|cli` 与启动描述在同一来源层重叠时也会拒绝；不凭字段值相同静默覆盖。解析后的副本一直用于本次 CLI 进程内调用，后续修改或删除启动描述不会改变已经解析的配置；独立诊断或 hook 子进程按自己的入口规则处理环境。help、schema 和 package 查询不创建这个业务 Context；run/launch 另按启动计划处理。

部署 Node 配置用于解析 CLI 的目标名称，例如 namespace `/deployed` 中的 `work` 解析为 `/deployed/work`。CLI 自己的控制 Node 使用根 namespace 下独立的 `/aos_cli_<UUID>` 身份，不会占用 provider 的 FQN。其 NodeCard 仍附带同一个 Context 的 `process` 元数据，表示所属进程与启动代次；这不改变目标名称，也不能单靠这份元数据满足 provider readiness，就绪探针还会核对目标 FQN、Node ID 和 endpoint。

## 启动依赖计划

`launch` 输入恰好选择一种来源：

```json
{"prefixes":["/opt/aos"],"path":"/opt/deploy/pair.json"}
```

```json
{"prefixes":["/opt/aos"],"package":"demo","resource":"pair"}
```

```json
{
  "prefixes":["/opt/aos"],
  "plan":{
    "schema_version":1,
    "context":{"domain_id":0,"zenoh_config":"/opt/aos/config/session.json5"},
    "processes":[
      {
        "id":"provider","package":"demo","entry":"provider",
        "readiness":{
          "node":"/demo/provider","timeout":10,
          "lifecycle":{"target":"active","autostart":true}
        }
      },
      {
        "id":"consumer","package":"demo","entry":"consumer",
        "depends_on":["provider"]
      }
    ]
  }
}
```

文件或包资源只读取一次，并与内联计划使用同一份 launch schema。文件内的相对 cwd、配置文件、Zenoh 配置和进程 `env` 中的 `AOS_ZENOH_CONFIG`、`AOS_INTERFACE_PATH` 按文档目录解析；内联计划中的相对路径按调用目录解析。CLI 的 `overrides.context.zenoh_config` 始终按调用目录解析。继承的环境路径按调用目录固定。所有路径在开始运行前固定。

`overrides` 使用 `{"context":{...},"nodes":{"process_id":{...}}}`，保留 CLI 来源。`context` 进入共享网络选择，`nodes` 按计划中的进程 ID 选择 Node 覆盖。被依赖的进程必须声明 readiness。计划中的重复 ID/FQN、依赖环、缺包、缺入口、非法版本或配置在任何 child 启动前拒绝。

每份计划最多 128 个进程；readiness、restart delay 和每项 shutdown 等待最多 86400 秒，SIGINT 与 SIGTERM 等待之和也不能超过 86400 秒。`max_restarts` 上限为 4294967294。声明 context 时必须明确提供 `domain_id` 和 `zenoh_config`，默认 Zenoh 配置使用 `null`。这些边界会在启动前校验，超过边界不会交给时钟或序列化代码处理。

`zenoh_config: null` 让控制 Context 与标准 SDK 使用同一份内置 `aos_zenoh::DEFAULT_SESSION_CONFIG`，其连接目标是本机 router `tcp/localhost:7447`；不会改用上游 Zenoh 的默认发现网络。

## 什么情况下才发出 ready

每个进程代次有独立的 steady 总期限。发现、NodeCard 请求、端点核对、生命周期查询和转换都消耗该期限；循环查询不会重新计时。

就绪探针按顺序核对：

1. 持久 Graph watch 中恰好有一个声明的 Node FQN，Graph Node 的 `id` 和 `node_id` 一致。
2. `get_node_card` 服务归属于该 Node，且使用标准接口的 type/hash。
3. NodeCard 的 FQN、Node ID 和 `process.launch_id/process_id/generation/pid` 与本次启动精确一致。缺少 process 元数据、同名外部 Node、旧 PID 或旧代次均不满足条件。
4. 声明端点在 Graph 和 NodeCard 中的 kind、name、type_name、type_hash 全部匹配。Service/Action 存在其他 provider 时拒绝歧义。
5. 若声明 lifecycle，则五个标准控制服务可用，`get_state` 返回目标状态。

`autostart=true` 可发送 configure、activate；每代每个转换最多发送一次。转换失败立即结束本次就绪。响应超时后，仅查询 provider 状态；不重复转换，也不宣称远端没有执行。状态读取明确返回 active 后才能放行要求 active 的消费者。

`ready` 是当前 provider 代次曾满足就绪条件的锁存结果。运行器不会持续监测该代次后续的生命周期变化；同一 provider 代次仍在运行时，新启动的消费者可以使用已有结果。provider 退出或重启后会清除结果，新代次必须重新证明就绪。

首次启动发现多个同名 Node，或 Service/Action 存在外部 provider 时，立即返回 `READINESS_FAILED`。运行器已观察到上一代退出后，下一代允许等待旧 Graph 条目撤销；重叠期间不会发出 `ready`，也不会发送生命周期转换。若歧义持续到该代的原始期限，返回带冲突原因的 `READINESS_TIMEOUT`。

探针不维护另一套生命周期或 Action 状态机。它使用一个共享控制 Context 和持久 Graph watch，每个进程代次最多一个 RPC worker，因此慢 provider 的转换不会阻塞其他 provider 的发现和状态查询。运行中和正在回收的 worker 合计最多 128 个，任务及结果队列均有容量限制；容量持续占满时，超时信息会明确指出 worker 上限。`poll` 只查看本地快照与已完成结果，不等待 RPC。`forget` 关闭退出实例的 Clients，忽略它的晚到结果。

关闭整个探针时，取消 Client、关闭 Context、释放 Graph/Node 和回收 worker 共用 1 秒的主线程等待上限。超过上限后，私有清理线程留待 CLI 退出时结束，并向已有的有限 stderr 队列写入 `probe_diagnostic`，其中 `data.code` 为 `PROBE_CLOSE_TIMEOUT`。诊断通道拥塞时计入丢弃，不延长关闭期限。正常完成路径会先清理运行器拥有的进程组，再关闭探针；提前退出的清理路径也使用同一探针关闭上限。

以上身份用于本机启动归属核对，不是认证凭证。

## 输出、停止与错误

stdout 每行都是 JSON。运行层事件带 `schema_version`、`launch_id`、`process_id`、`generation`、`event`、`elapsed_ms`；已知时附 PID/PGID。主要事件为：

| 事件 | 含义 |
|---|---|
| `process_started` | OS 已创建 child |
| `discovered` | 已确认属于当前进程代次的 NodeCard |
| `lifecycle_transition` | 正在推进一次生命周期转换 |
| `ready` | 本次声明的全部就绪条件已满足 |
| `process_stdout` | child 的完整 JSON 值，位于 `details.value` |
| `output_truncated` / `output_dropped` | 超长输出或诊断背压的明确计数 |
| `restart_scheduled` / `restart_exhausted` | 有限重启的安排或额度耗尽 |
| `process_stopping` / `process_signal` | 本次进程组进入关闭或收到信号 |
| `launch_finished` | 运行层已结束清理，`details.summary` 是摘要 |
| `summary` | CLI 的终态摘要，位于 `data` |
| `error` | CLI 错误外壳，位于 `data` |

child 的普通 stdout 文本和 stderr 进入父进程 stderr 的 `process_log` JSON，不混入业务输出。合法 JSON 的原始数字表示会保留；重复键不会被静默覆盖。stdin 不作为 child 命令通道。

CLI 用独立写线程和有限队列输出；Unix 上不改动 stdout/stderr 共享的 `O_NONBLOCK` 标志。stdout 队列最多 4 MiB；下一帧因队列已满无法入队后，连续 30 秒没有任何写入进展才触发输出失败。每次成功写入都会重新计时；仅有少量尚未写出的数据不会触发这个期限。短暂停读会保留当前 JSONL 行的位置并恢复写入，child 的 stdout 管道提供背压。管道断开则立即进入失败清理。输出拥塞期间仍处理停止信号和进程组回收。

这 30 秒只限制运行中的无进展停滞，持续慢速写入不会触发它。停止请求另外启动绝对期限：配置的 INT/TERM 宽限加 2 秒组回收，再加 30 秒输出排空；即使输出仍在缓慢前进，也不会延长该期限。到期丢弃待交付输出并返回 `OUTPUT_ERROR`；已有输出错误也不会跳过已回收进程读端的关闭。

leader 回收后，每个流各有原始字节额度，每个进程有一个 2 秒管道关闭窗口。运行器先按回收时的可读字节快照（Unix `FIONREAD`、Windows `PeekNamedPipe`）排完内核和本地残留，允许每轮读取配额及多进程轮转延长这段有限工作；后来写入的数据不能补充快照。残留排完并检查尾部后，新字节不再延长关闭窗口。sink 背压或待交付事件可以延后这项检查，但上述情形均不能延长绝对停止期限。超额或超时的组外写者触发 `CLEANUP_FAILED`，慢速 sink 本身不触发这个清理错误。

stderr 使用独立的 1 MiB 队列，诊断拥塞或断开时计数丢弃，并通过 stdout 的 `output_dropped` 报告，不停止业务进程。合成的丢弃事件沿用最近一个运行层事件的 `elapsed_ms`，时间基准与 supervisor 一致。结束时诊断排空最多等待 100 毫秒，尚未完整写出的记录也计入丢弃。所有终态 summary/error，包括输入校验失败，共用另外一个最多 2 秒的写出期限。运行层 `poll` 出错时也保留 `launch_summary` 和终态 summary，并在该期限内尝试写出；失败的输出通道可能无法承载终态摘要，不应等待不存在的最后一行。stdout 与 stderr 必须分别消费或保存；`2>&1` 合并后，两个写线程的长记录可能交错，不能再保证合并流是完整 JSONL。

终态写出失败不覆盖已有的命令错误或信号退出码。例如已按 SIGTERM 收尾得到 143，最后的 stdout 写出失败仍保留 143；只有命令原本成功时，终态输出失败才把退出码改为 1。运行期间或绝对停止期限内发生的输出失败则属于运行结果本身。

建立控制 Context 期间尚未创建 child 或运行目录，保留 OS 默认中断行为。setup 完成后、第一次可能创建 child 的 `poll` 前，CLI 安装清理 handler。Unix 上 SIGHUP、SIGINT、SIGQUIT、SIGTERM 请求停止自有进程组，关闭按依赖逆序执行 SIGINT→SIGTERM→SIGKILL。Windows 上 Ctrl-C/Ctrl-Break 请求停止，运行器在两个配置宽限阶段发送 Ctrl-Break，最终通过 Job Object 终止仍存活的自有进程树；不把这些操作称作 Windows 的 POSIX 信号。

超时、转换失败、启动失败和输出失败均不会授权重放业务调用。Unix 上脱离所属组的 daemonize 进程不在此承诺范围；交互运行的 supervisor 若被 SIGKILL 或 OOM 强制终止，也无法保证 child 回收。Linux 的 systemd/cgroup 负责覆盖 supervisor 崩溃后的清理。Windows 的 Job 设置在最后一个 Job 句柄关闭时终止后代，重新创建控制台组不会脱离该 Job。若 OS 管理器随后重启 supervisor，会重新执行整份计划，一次性消费者也会再次运行；有外部副作用的消费者需要自行使用幂等键或持久完成记录，不能把 OS 重启当成仅恢复 provider。

摘要中的 `outcome` 区分 `completed`、`stopped`、`failed`，并列出实际代次数、重启次数、最后退出状态和清理错误。`processes[].ready` 是摘要时保留的运行层状态；判断“本代是否曾成功就绪”应查看该代的 `ready` 事件。

| 退出码 | 含义 |
|---|---|
| 0 | 正常完成或无信号的显式停止 |
| 1 | 进程、转换、清理或输出失败 |
| 2 | 非法输入、版本/ABI 或不支持平台 |
| 3 | 缺包、缺入口、缺资源或歧义 |
| 4 | 就绪等待超时 |
| 129 / 130 / 131 / 143 | Unix SIGHUP / SIGINT / SIGQUIT / SIGTERM，已执行本次清理流程 |
| 130（Windows） | 控制台 Ctrl-C/Ctrl-Break，已执行本次清理流程 |

CLI 与运行层测试分别覆盖输入接线和进程语义。真实 SDK 配置注入、最终 wheel/bundle 安装、Linux systemd、跨主机运行仍需使用对应交付版本单独验收；模拟 provider 和启动描述 fixture 不能替代这些结果。
