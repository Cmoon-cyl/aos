# SQLite MemoryServer：普通程序业务示例

MemoryServer 是一个使用 AOS SDK 的常驻普通程序。它把调用方提交的便笺保存在本地 SQLite，通过 Service 提供存取，通过 Topic 发布运行计数，通过 Action 生成真实 JSONL 快照。代码位于 `examples/memory_server/`，安装器为 `scripts/install_memory_package.py`。

这个示例用于验证应用如何使用已有平台能力。SQLite 只保存业务数据和写请求的幂等结果；生命周期和 Action goal 状态仍由 SDK/Rust 管理。它不提供中央 daemon、GoalStore、旧 Action 恢复、文件传输或自动重试。

## 从安装到第一次调用

需要匹配平台的 AOS wheel、对应 CLI，以及调用方选择的 Zenoh session 配置。各平台的原生实现和实际验收范围见[支持范围](../support.md)。工作目录必须为空、尚不存在，或者已由本示例创建；示例不会导入已有笔记。测试只使用自有合成便笺。

以下路径由部署者填入。安装器不会启动 Node、连接 Router 或打开工作目录，也不会下载依赖。已有安装 prefix 一律拒绝覆盖。

```bash
MEMORY_PYTHON=/absolute/installed-env/bin/python
MEMORY_AOS=/absolute/bin/aos
MEMORY_PREFIX=/absolute/new-memory-prefix
MEMORY_DATA=/absolute/owned-memory-data
MEMORY_CONFIG=/absolute/zenoh-session.json5

"$MEMORY_PYTHON" -I /absolute/aos/scripts/install_memory_package.py \
  --prefix "$MEMORY_PREFIX" --work-dir "$MEMORY_DATA" \
  --config "$MEMORY_CONFIG" --domain-id 2021 --namespace /memory
```

Windows PowerShell 使用同一个安装器与生成的 JSON 请求：

```powershell
$MemoryPython = "C:/aos/venv/Scripts/python.exe"
$MemoryAos = "C:/aos/bin/aos.exe"
$MemoryPrefix = "C:/aos/memory-prefix"
$MemoryConfig = "C:/aos/config/client.json5"
& $MemoryPython -I C:/aos/scripts/install_memory_package.py `
  --prefix $MemoryPrefix --work-dir C:/aos/memory-data `
  --config $MemoryConfig --domain-id 2021 --namespace /memory
& $MemoryAos launch start --request "$MemoryPrefix/share/aos_memory/requests/launch-pair.json"
```

把示例路径替换为已解压发行包、已安装 wheel 的 venv 和新工作目录；先启动该配置指向的 Router。

包名为 `aos_memory`，提供 `provider`、`consumer` 和只读 `request_observer` 入口，以及 Put/Get/List/Status/Export 五个业务接口和固定 RequestTelemetry 观察接口。入口使用安装时的绝对 Python 和 `-I`，可以从任意工作目录运行；不会从当前目录或 `PYTHONPATH` 导入 SDK。

请求诊断默认关闭。安装时显式加 `--export-request-telemetry`，会给 provider 和 consumer 都配置 `--telemetry-topic`，导出其真实 Core Service/Action metadata，不从 SQLite audit 日志重构。capture/drain 单 owner、独立发布 Context、精确 ACL、只读观察与回放验收见 [请求诊断说明](request-observer.md)。诊断状态单独报告，不改变业务返回结果，也不提供业务重放。

启动一组 provider 和一次性消费者：

```bash
"$MEMORY_AOS" launch start \
  --request "$MEMORY_PREFIX/share/aos_memory/requests/launch-pair.json"
```

supervisor 发现本次进程对应的 Node，核对五个业务端点，执行 configure/activate 并确认 active 后，才放行消费者。消费者依次提交三条合成便笺、读取一条、列出三条、导出一个快照，再接收两条 status。只有这些实际业务结果都满足预期，才输出 `demo_completed`。消费者退出后 provider 继续运行；按 Ctrl-C 请求整组停止，并检查最终 `summary` 的 `cleanup_errors`。

也可分开使用两个终端。先启动 provider，等到 `ready` 后再运行消费者：

```bash
# 终端 1：保持运行，先等 ready。
"$MEMORY_AOS" run start \
  --request "$MEMORY_PREFIX/share/aos_memory/requests/run-server.json"

# 终端 2：完成一次确定性业务后退出。
"$MEMORY_AOS" run start \
  --request "$MEMORY_PREFIX/share/aos_memory/requests/run-consumer.json"
```

安装器还生成 `launch-server.json`，用于只启动 provider。两个 launch 资源都没有隐藏的业务重放。provider 意外非零退出后最多重启两次，每代重新满足 readiness；消费者的 restart policy 为 `never`。

直接使用一次性消费者时，显式选择同一个网络、domain 和 provider namespace。例如，在已 active 的 provider 上创建一条便笺：

```bash
AOS_PREFIX_PATH="$MEMORY_PREFIX" "$MEMORY_PREFIX/lib/aos_memory/consumer" \
  --config "$MEMORY_CONFIG" --domain-id 2021 --namespace /memory \
  put --request-id sample-001 --key synthetic/first --text 'synthetic note'
```

Windows 直接调用消费者时设置 `$env:AOS_PREFIX_PATH = $MemoryPrefix`，然后执行：

```powershell
& $MemoryPython -I -u "$MemoryPrefix/lib/aos_memory/consumer.py" `
  --config $MemoryConfig --domain-id 2021 --namespace /memory `
  put --request-id sample-001 --key synthetic/first --text "synthetic note"
```

同样的入口提供 `get --key ...`、`list --prefix ...`、`export --prefix ...`、`observe --count 2`，以及 `demo --request-prefix ...`。全局参数放在子命令前。每次 Service 调用和每个 Action goal 都只提交一次；`observe` 在一个总超时内接收指定数量的样本。默认超时为 10 秒，允许范围为 `(0, 30]` 秒。观察一秒一条的 status 时，要给样本数量留足时间。

消费者退出码为：0 表示所选操作得到预期成功或真实 CANCELED 终态，1 表示已知业务拒绝/失败，2 表示调用或资源清理异常。`consumer_error` 只输出异常类型及待确认的 request_id/goal_id，不把超时改写成回滚成功。

## 接口与数据契约

默认 Node 为 `/memory/server`；安装时可以修改 namespace。NodeCard 和端点描述包含生命周期要求、副作用、契约版本 `1.0.0`、接口 schema/default 和幂等边界。陌生 Agent 可先查询 CLI help/schema，再发现 Node、读取 description，按实际发现的接口调用，无需预先知道端点。

| 端点 | 类型 | 输入与结果 | 副作用 |
|---|---|---|---|
| `/memory/put` | Service | `request_id,key,text,expected_revision=0`；返回 revision、commit_seq、UTF-8 字节数和 SHA-256 | 写入 SQLite |
| `/memory/get` | Service | `key`；返回完整便笺及 revision/hash；不存在时 `NOT_FOUND` | 只读 |
| `/memory/list` | Service | `prefix=''`、排他游标 `after_key=''`、`limit=50`；返回不含正文的有序元数据、`next_after`、`has_more` | 只读 |
| `/memory/status` | Topic | active 期间每秒发布 instance_id、sequence、commit_seq、记录/字节/请求数、active_exports、audit_errors | 只读发布 |
| `/memory/export` | Action | `prefix=''`、`max_records=10000`；返回 provider 上的相对文件名、实际记录/字节数、SHA-256、snapshot_seq、instance_id | 创建完整导出文件 |

Service 业务响应及 Export 业务结果统一为 `{ok,code,message,data}`。`ok=false` 是已返回的业务结果；RPC 超时、schema 验证失败、权限或连接问题由 AOS 传输/控制层另行报告。不能把一次传输失败直接当作某个业务错误码。

`key`、`request_id`、分页游标和非空 prefix 使用 1–128 字符的 ASCII 标识符：首字符为字母或数字，后续可包含字母、数字、`_./:-`。key 是数据库键，不作为文件路径。text 必须为非空、可编码为 UTF-8 的字符串，受当前单条字节限额约束。list 的 limit 范围为 1–200，按 key 排序，prefix 是字面前缀；每次调用内读取一致，跨页不承诺同一个快照。

### 写请求如何保持幂等

`expected_revision=0` 只允许创建。更新必须先获取当前 revision，再用新的 request_id 和这个 revision 提交；成功后 revision 加一。

一个 SQLite 事务同时提交便笺变化、计数和 request_id 的规范参数指纹/业务结果。相同 ID 和参数再次出现时，返回第一次已保存的结果；即使便笺后来被更新或进程已重启，也不会重新执行。相同 ID 搭配不同参数返回 `REQUEST_ID_CONFLICT`。revision 不匹配返回 `REVISION_CONFLICT`。

通过输入校验、且账本尚有容量的请求，其 revision/capacity 拒绝也会写入幂等账本；随后改变容量或数据，不会把相同 ID 的旧拒绝转成一次新写入。输入无效或账本已经满的请求不新增账本行。账本不自动淘汰：满后仍可读取、重放既有 ID，也可在 inactive 时显式提高容量。

**写超时仍可能已提交。** 调用方应保留提交前输出的 request_id，并由调用方明确决定是否用完全相同的 ID/参数再次确认。消费者不会在超时、重连或 provider 重启时自动执行这一步。改变参数前需取得当前状态并选择新的 request_id。

### 导出、进度和取消

Export 打开独立的 SQLite 只读事务，固定 snapshot_seq，按 key 顺序逐批写出真实记录。JSONL 每行包含 key、text、revision、sha256、text_bytes、commit_seq 和 updated_unix_ns。feedback 中的 `records_written/total_records/bytes_written/snapshot_seq` 来自实际已写批次；feedback 不代表文件已发布。

导出先写同目录的 `.pending-<goal_id>.jsonl`，检查记录数和实际编码字节数，flush/fsync 完成后，再原子地发布 `exports/<goal_id>.jsonl`。发布不覆盖同名文件；复用已经存在的文件名返回 `EXPORT_EXISTS`。result 的 SHA-256 是实际 JSONL 字节的摘要，文件仍位于 provider 主机，接口不传输文件。

取消回调与文件发布使用同一把业务锁。取消回调先记录“不要发布”的业务意图，再返回给 SDK；因此即使 Rust 尚未进入 CANCELING，发布也会被阻止。执行回调等待实际 native 决议并调用 `goal.canceled()`；只以 GetResult 返回的真实 CANCELED 终态证明取消成功。这个短暂的业务标记不保存或恢复 Action 状态。

文件已发布后，取消回调拒绝取消，成功路径最终为 SUCCEEDED。导出很小时可能先完成：消费者的 `--cancel-after-feedback 1` 会如实输出取消回复和终态，不保证总能取消。未达到指定 feedback 数量、但已收到终态时，不会继续等待或制造一个取消成功结果。

发布前失败、取消或关闭会删除临时文件，不留下半份正式文件。发布之后若目录同步等 I/O 失败，可能已有完整正式文件，但 Action 返回 `STORAGE_ERROR`；应按打印的 goal_id 检查已拥有的目录，不能据此自动再导出。测试中的批次/发布闸门只在测试构造函数注入，CLI 不提供这些选项，也不通过 sleep 伪造长工作。

每个新 goal 是独立导出；put 的 request_id 幂等不延伸到跨 goal。Action 结果在当前 server 实例中进入终态后保留 60 秒，受 SDK 的 128 个 goal、512 KiB 结果缓存及整个 server 合计 32 个 GetResult 等待者上限约束。重启或结果保留期到期后，旧 goal 可以变为 UNKNOWN；已有完整导出文件仍保留。这里没有累计导出目录容量管理或自动删除策略，部署者需为整个工作目录安排磁盘配额/清理。

本示例一次只准入一个正在执行的导出。它使用 LifecycleNode 的默认互斥 Action 控制组：SDK 在同一次持锁回调内依次调用 goal_callback、决定是否受理，再调用 handle_accepted_callback；后者先登记当前导出，再调度执行。因此“检查空闲”和“登记导出”之间不会插入第二个 goal 控制回调。不要将这个默认组替换成可重入组后仍假定该串行保证成立；业务发布锁另外负责执行与取消的竞态。

## 生命周期、参数和文件边界

provider 启动时为 unconfigured。configure 检查数据库现有用量，activate 打开业务准入。inactive 时保留 Node/参数/lifecycle 管理能力；业务 Service/Action 和 status 的准入由 LifecycleNode 管理，不在 Python 中绕过。status 只在 active 期间持续发布，包括没有消费者时；inactive 期间不积压历史样本等待激活后重播。

以下参数只允许在 inactive 时原子修改。单次修改中的任一值不合法或低于已提交用量，整批拒绝。

| 参数 | 默认值 | 允许范围及单位 |
|---|---:|---|
| `max_record_bytes` | 16 KiB | 1–65,536，单条正文 UTF-8 字节数 |
| `max_records` | 10,000 | 1–100,000，便笺键数量 |
| `max_total_bytes` | 64 MiB | 1–256 MiB，已提交正文 UTF-8 字节总数 |
| `max_requests` | 100,000 | 1–1,000,000，持久化请求结果数量 |
| `export_batch_size` | 64 | 1–512，每批实际读取/写入的记录数 |
| `export_bytes_limit` | 64 MiB | 1–64 MiB，单个完整 JSONL 文件的编码字节数 |

这些是业务上限；`max_total_bytes` 不包括 SQLite 索引/WAL、幂等账本、audit 和导出文件的磁盘开销。参数通过标准 AOS 配置和远端参数接口管理。应用不把运行时参数偷偷写入业务数据库；重启按部署配置重新解析参数。若新的限制小于旧数据用量，configure 失败，需明确调整参数后再激活。

只操作调用方明确选择的工作目录中的以下自有文件：

| 文件 | 用途与重启行为 |
|---|---|
| `.aos-memory.json` | 格式与 store_id；拒绝把普通非空目录当作数据库 |
| `.aos-memory.lock` | 一个进程独占写入该目录；第二个进程启动失败 |
| `memory.sqlite3` 及 `-wal/-shm` | 业务记录、请求账本和计数；WAL、synchronous=FULL |
| `exports/<goal_id>.jsonl` | 已发布的完整快照；重启保留 |
| `exports/.pending-<goal_id>.jsonl` | 未完成文件；取得目录锁后，仅清理本格式的临时文件名 |
| `audit.jsonl`、`audit.previous.jsonl` | 最多两份、每份 256 KiB；包含 instance_id、request_id/goal_id、commit_seq 和事件，不含便笺正文 |

每次启动生成新的 instance_id，已提交数据的 store_id 和 commit_seq 保持。数据库身份、schema 或目录标记不匹配时拒绝打开，不重建覆盖。Unix 使用 `flock`，Windows 使用一字节非阻塞独占文件锁，锁随进程关闭释放。受管数据库、exports 和 audit 路径拒绝符号链接；Windows 也拒绝重解析点。audit 写失败通过计数暴露，不回滚已经提交的业务结果。

## 原生 TLS 部署

安全模式使用三份调用方预先准备的 profile，分别用于 supervisor、provider、consumer。安装器只检查明确的路由选择一致并记录路径；证书/密钥的实际可用性、身份及权限由 native TLS/ACL 路径验证。

```bash
"$MEMORY_PYTHON" -I /absolute/aos/scripts/install_memory_package.py \
  --prefix /absolute/new-secure-memory-prefix --work-dir /absolute/owned-secure-data \
  --supervisor-profile /absolute/supervisor.json \
  --provider-profile /absolute/provider.json --consumer-profile /absolute/consumer.json \
  --namespace /memory --client-name memory_supervisor --client-namespace /memory_control
```

生成的 request 显式选择 supervisor profile 和 `client_node`；每个 child 在自己的 `env.AOS_SECURITY_PROFILE` 中选择 profile，不继承 supervisor 凭据代替自己的身份。三份 profile 必须选择相同 domain 和 Router endpoint。prefix 中只保存 profile 路径，不复制证书或私钥。安全 preflight 和实际连接仍会检查继承环境及最终配置；不要将某个继承的安全配置静默清空以强制回退到普通连接。

策略需要覆盖实际 Node 身份和操作：provider 的 Node/参数/lifecycle 管理、三个 Service、Status 发布、Export server；consumer 的 Node/参数管理、Service 调用、Status 订阅以及 Action 发送/结果/取消；supervisor 的发现、描述和所选 lifecycle 控制。NodeCard 能被看到、Router 能连接、readiness 通过，都不能单独证明这些业务操作已授权。权限拒绝须结合 Router 拒绝记录和业务状态不变确认。

## 回归与验收范围

业务测试包含真实事务回滚、重复 request_id 并发、重启后重放、字节/账本容量、稳定分页、SQLite 一致快照、I/O/容量失败无半成品、原子发布与取消竞态、有限 audit，以及真实 SDK 的生命周期、Service、Action 终态和消费者超时不重放。`test_memory_install.py` 额外验证新 prefix、绝对解释器、资源完整性和三份 profile 的显式分离。

```bash
# 当前 Python 必须已经安装待测 AOS wheel。
"$MEMORY_PYTHON" -I -Werror::ResourceWarning -m unittest discover \
  -s /absolute/aos/python/tests -p 'test_memory_*.py' -v

# 增加真实安装后 run/launch 检查：使用自有 loopback Router 和临时合成数据。
AOS_CLI="$MEMORY_AOS" AOS_ZENOHD=/absolute/zenohd \
  "$MEMORY_PYTHON" -I -Werror::ResourceWarning -m unittest discover \
  -s /absolute/aos/python/tests -p 'test_memory_*.py' -v
```

未设置 CLI/Router 时，两个真实进程用例会明确跳过，不能计作已执行。部署时还应使用同一发行版的 CLI/wheel 检查 run/launch、持久化和恢复；跨机与 TLS 需要按实际拓扑运行。
