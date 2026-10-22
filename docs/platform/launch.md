# 安装包、run 与 launch 运行契约

本页定义 0.2.0 的运行契约；各平台原生验证与系统集成范围见[发行说明](../releases/v0.2.0.md)和[支持范围](../support.md)。`crates/aos-launch` 负责本地进程；core/PyO3 在标准初始化路径消费启动描述；CLI 使用真实 Graph 和生命周期服务检查归属与就绪。运行层测试、SDK 安装测试和 systemd 夹具测试分别记录证据，不能相互代替，也不能替代同一候选上的真实程序验收。

本期目标是：安装后从任意 cwd 找到包、入口和资源；部署覆盖自动进入使用标准 SDK 初始化的 Node；在本机按真实就绪条件启动依赖进程；有界关闭本次启动的进程组，按明确策略有限重启。supervisor 不恢复旧进程的 Action goal，也不自动重试业务调用。

## 固定来源与实际采用范围

取证日期：2026-10-21。下列链接均固定到本次实际读取的提交。

| 来源 | 已核对行为 | AOS 采用与差异 |
|---|---|---|
| ament_index Jazzy [`2ccdcb8` resources.py](https://github.com/ament/ament_index/blob/2ccdcb8fd19a364fc793273b5793459207a440b9/ament_index_python/ament_index_python/resources.py)、[search_paths.py](https://github.com/ament/ament_index/blob/2ccdcb8fd19a364fc793273b5793459207a440b9/ament_index_python/ament_index_python/search_paths.py)、[packages.py](https://github.com/ament/ament_index/blob/2ccdcb8fd19a364fc793273b5793459207a440b9/ament_index_python/ament_index_python/packages.py) | 环境变量提供安装前缀；资源索引按类别/名称定位；包资源在 `share/<package>`。多个前缀同名时返回首项 | 采用前缀和静态索引。按 AOS 的包索引规则，同名包歧义报错，不能把 ament 的首项遮蔽行为直接搬入 |
| ros2cli Jazzy [`a71e5b3` ros2pkg/api](https://github.com/ros2/ros2cli/blob/a71e5b3c982384aa8081202f8b21a189f0eb7ce7/ros2pkg/ros2pkg/api/__init__.py)、[ros2run/api](https://github.com/ros2/ros2cli/blob/a71e5b3c982384aa8081202f8b21a189f0eb7ce7/ros2run/ros2run/api/__init__.py) | 从包前缀的 `lib/<package>` 找可执行文件；同名多个入口拒绝；以 argv 数组启动并等待退出 | AOS manifest 明确列入口，不递归扫描猜测；仍使用绝对入口路径和 argv，不经过 shell |
| launch_ros Jazzy [`97b7584` node.py](https://github.com/ros2/launch_ros/blob/97b758478d8bf343a1e83ab419210d790da53d80/launch_ros/launch_ros/actions/node.py)、[executable_in_package.py](https://github.com/ros2/launch_ros/blob/97b758478d8bf343a1e83ab419210d790da53d80/launch_ros/launch_ros/substitutions/executable_in_package.py) | 找包入口，将名称、namespace、参数文件、remap 转成 `--ros-args`，再交给通用进程执行器 | 复用“运行层产生部署输入、SDK/core 消费”的责任划分。AOS 使用保留来源层次的 JSON 启动描述，不实现 ROS 参数语法 |
| rclpy Jazzy [`4e607c0` context.py](https://github.com/ros2/rclpy/blob/4e607c09d624debc39a4ae5405680c06d87fac35/rclpy/rclpy/context.py) | `Context.init(args=None)` 把 `sys.argv` 交给 native Context | AOS 的配置注入必须进入标准 SDK 初始化路径，不能依靠每个示例自行解析启动参数 |
| launch_ros Jazzy [`97b7584` lifecycle_node.py](https://github.com/ros2/launch_ros/blob/97b758478d8bf343a1e83ab419210d790da53d80/launch_ros/launch_ros/actions/lifecycle_node.py)、[lifecycle_event_manager.py](https://github.com/ros2/launch_ros/blob/97b758478d8bf343a1e83ab419210d790da53d80/launch_ros/launch_ros/utilities/lifecycle_event_manager.py) | Lifecycle 控制通过服务与 transition_event 完成；可选 autostart 执行 configure、activate；等待服务/响应时检查关闭 | AOS 使用现有生命周期服务和状态，等待有 steady 总上限；不能由 process_started 推断 active |
| launch Jazzy [`41d58e3` execute_local.py](https://github.com/ros2/launch/blob/41d58e36823026aa81955133c3b27d8baa596899/launch/launch/actions/execute_local.py) | 分别捕获 stdout/stderr，产生 started/exited 事件；关闭按 SIGINT→SIGTERM→SIGKILL；重启等待响应 shutdown | 采用事件和关闭顺序。源码的 respawn 分支没有检查 returncode，且负重试上限代表无限次；AOS 的 `on_failure` 明确只匹配失败退出，拒绝无限重启 |
| launch_ros Jazzy [`97b7584` ros2launch/api](https://github.com/ros2/launch_ros/blob/97b758478d8bf343a1e83ab419210d790da53d80/ros2launch/ros2launch/api/api.py) | 在包 share 目录找 launch 文件，同名文件多处出现报错 | AOS 以 manifest 中命名资源直接定位，只读取声明式 JSON |

本稿只迁移机制和责任边界，不复制上游实现。发生后续代码迁移时另补许可证和 `upstream/` 归属记录。

## 安装布局与发现

安装前缀布局如下：

```text
<prefix>/
  bin/aos
  lib/<package>/<entry>
  share/aos/resource_index/packages/<package>.json
  share/<package>/interfaces/*.json
  share/<package>/config/*.json
  share/<package>/launch/*.json
```

资源索引本身就是严格 JSON manifest，不另引入可执行安装脚本：

```json
{
  "schema_version": 1,
  "name": "aos_demo",
  "version": "0.2.0",
  "runtime_abi": "aos.process.v1",
  "entries": {
    "worker": {"executable": "lib/aos_demo/worker", "kind": "aos_node"}
  },
  "resources": {
    "work": {"kind": "interface", "path": "share/aos_demo/interfaces/Work.json"},
    "pair": {"kind": "launch", "path": "share/aos_demo/launch/pair.json"}
  }
}
```

- 前缀来自本次 CLI 显式列表；未提供时读取 `AOS_PREFIX_PATH`，再加入已安装 `aos` 自身的前缀。显式前缀模式不再混入隐式路径。相同 canonical 前缀去重，绝不把 cwd 自动当安装前缀。
- `PackageIndex::prefixes()` 返回本次固定的 canonical 前缀。supervisor 用平台 path-list 格式将这份列表写入每个 child 的 `AOS_PREFIX_PATH`，覆盖父进程旧值，供 SDK 包资源 API 使用；`process.env` 不能覆盖该保留键。不能表示为 path-list 的前缀在 prepare 阶段拒绝。
- CLI 和 SDK 共用 Rust `resolve_package_prefixes`。普通 SDK 默认读取环境前缀并加入 `sys.prefix`；带 `AOS_PROCESS_CONFIG` 的 child 只使用 supervisor 固定的环境列表，不能再混入解释器前缀。整串空 `AOS_PREFIX_PATH` 视为未设置；非空 path-list 内的空项拒绝。相对前缀按调用时 cwd 固定，不把 cwd 本身当作隐式前缀。
- 每个包名只允许一个命中的 manifest。不同前缀出现同名包时报 `ambiguous_package`，列候选前缀/版本；用户用显式前缀消歧。不能默选较新版本。
- `schema_version` 和 `runtime_abi` 必须受支持；可选的请求 `package_version` 是精确匹配，不引入版本求解器。缺包、缺入口、版本不符、ABI 不支持是启动前错误。
- manifest 的包名、入口名、资源名必须是非空标识符，不能含路径分隔符或 `.`/`..`。`executable` 和资源路径相对安装前缀；canonical 路径必须仍位于该前缀中，且类型/权限正确。声明到包外的入口或资源拒绝。
- 解释型入口可以显式声明 `interpreter` 和 `interpreter_args`。解释器必须是已存在的绝对可执行文件，可以位于包外；运行参数顺序为解释器参数、已解析入口路径、业务参数。不声明解释器时，`interpreter_args` 必须为空。Windows 示例安装器使用 `.py` 入口和安装 wheel 的绝对 Python，不依赖 shell 文件关联。
- 接口资源按原始 Interface 解析并核对类型/hash；资源查找不 import Python 模块、不执行入口、不执行 launch 文件。
- 示例安装器先安装入口/资源，最后写入索引 manifest；SDK 的包资源 API 读取相同索引。该约定不意味着任意 pip 包已经自动注册。

## 配置注入：必须在标准初始化路径完成

core 的 `ContextConfigRequest` 与 `NodeConfigRequest` 分别接收 `program/config_files/environment/launch/cli`。标准 Python Context 与 Rust `open_context` 自动加载启动描述；使用这些入口的应用无需自行解析部署参数。精确入口与验证规则见[进程启动配置](process-bootstrap.md)。

每次进程启动都有独立的描述文件，以保留的环境变量 `AOS_PROCESS_CONFIG` 传绝对路径。该变量只定位输入，不被当成“环境配置层”；文件中的字段保留各自来源。文件由 supervisor 写入本次运行目录；Unix 上目录权限为 0700、文件权限为 0600，Windows 使用目录继承的访问控制，不将 POSIX 权限位当成 Windows ACL 证明；限制为 1 MiB，严格拒绝重复键、未知字段和不支持版本，不展开变量或执行内容。

```json
{
  "schema_version": 1,
  "launch_id": "606c355e-00a9-405b-9f41-c5855426dfe2",
  "process_id": "provider",
  "generation": 1,
  "context": {
    "config_files": [],
    "launch": {"domain_id": 0, "zenoh_config": "/opt/aos/config/client.json5"},
    "cli": {}
  },
  "node": {
    "config_files": [],
    "launch": {
      "nodes": {
        "worker": {
          "namespace": "/demo",
          "parameters": {"limit": 3},
          "remaps": [{"kind": "service", "from": "work", "to": "/demo/work"}]
        }
      }
    },
    "cli": {}
  }
}
```

输入路径在 launch/run 解析时固定为绝对路径；相对 launch 资源路径按该资源所在目录解析。继承环境中的 `AOS_ZENOH_CONFIG` 和 `AOS_INTERFACE_PATH` 按调用 cwd 固定；文档内 `process.env` 的同名路径按文档目录固定。之后切换 child cwd 不改变配置指向。运行目录也会 canonicalize，因此相对 `TMPDIR` 不会产生相对 bootstrap 路径。

标准初始化使用以下 core API：

```rust
// aos-core：纯解析、来源合成；不启动进程，不打开 Zenoh。
pub struct ProcessBootstrap { /* 对应上述 v1 JSON，字段严格 */ }
pub struct DeploymentLayers { pub config_files: Vec<String>, pub launch: Value, pub cli: Value }
impl ProcessBootstrap {
    pub fn from_environment() -> Result<Option<Self>>;
    pub fn context_request(&self, local: ContextConfigRequest) -> Result<ContextConfigRequest>;
    pub fn node_request(&self, local: NodeConfigRequest) -> Result<NodeConfigRequest>;
}
// Context 保存只读 bootstrap，Node 普通/生命周期构造路径统一使用。
```

这些 API 的行为必须是：

1. **Python/PyO3 Context 构造时自动加载。** 先解析启动描述并调用 core 合成 Context 请求，之后才打开 adapter。把同一份只读 bootstrap 保存在该 Context，后续所有 Node 自动使用，不要求用户业务代码增加启动参数。
2. **Node 在建实体前合成。** 用户构造参数仍是 program；API 指定的配置文件先于启动描述中的文件；实际环境白名单仍为第 4 层；启动描述的 launch 与 cli 分别进入第 5、6 层。`Node(...)` 和 `LifecycleNode(...)` 行为一致。
3. **不预先折叠层次。** 不把全部覆盖变成 `AOS_NODE_NAME` 或单个 program 对象；`effective_config` 保留 `file:<path>/environment/launch/cli`。节点选择仍按声明时 name/FQN，最终名称变化不会改变 selector 的含义。
4. **同来源合成不得偷偷覆盖。** 应用 API 与启动描述在同一个 launch/cli 层提供重叠叶字段时拒绝，即使值相同；互不重叠的对象字段可合并。数组被两个同层来源同时提供也拒绝。跨层仍沿用配置覆盖和专用 remap 规则。
5. **参数保持声明与生命周期语义。** 启动参数只作为声明初值覆盖；不自动声明任意参数，也不绕过类型、range、validator。启动覆盖不受 `allowed_states` 的后续修改限制。
6. **一个进程可以创建多个 Node。** 共享 bootstrap 的 node 对象，按各自声明身份选配置；不依赖第几个构造的 Node，也不消费一次后清空。程序显式创建的另一个 Context 同样按标准初始化加载。
7. **标准 Rust 初始化已接入。** adapter 的公共入口 `open_context(ContextConfigRequest)` 先加载 bootstrap/解析，再打开 Zenoh，并把 bootstrap 交给 Context。现有 `Context::new(already_open_transport)` 是底层入口，无法改变已经打开的网络会话；这个入口不自动完成 Context 网络注入。
8. **旧或普通进程边界明确。** 当前仅开放 `kind=aos_node`，要求支持 `aos.process.v1` 的标准 SDK 初始化。`kind=process` 尚未开放，运行器不会给不支持的入口伪造“配置已应用”事件。

为避免同名的其他进程或重启前实例满足 readiness，core 已在 NodeCard 中增加可选 `process` 元数据：`launch_id/process_id/generation/pid`。它来自已载入的 bootstrap 和本进程 PID；不是用户身份或认证凭证。supervisor 只接受与本次子进程及 generation 一致的 NodeCard，并将该 Card 的 `id` 与 Graph 的 `node_id` 对齐。Graph 沿用现有协议字段。

运行层在 prepare 时用 core 的 `ProcessBootstrap::from_json` 校验模板，写入每代文件前再次校验完整描述，防止两侧 DTO 漂移。SDK 安装测试核对标准 Context/Node 确实消费该输入。

## 最小 run/launch 请求

CLI 负责现有 JSON 命令解析与输出外壳，runtime 消费强类型请求。`run` 等价于一个进程的 launch 计划；两者共用解析、启动、关闭和事件路径，避免出现两套行为。

下面是 launch 数据结构示例。带 readiness 的计划显式提供完整共享 `context`（`domain_id` 和允许为 null 的 `zenoh_config`）；其值进入每个 child 的 launch 层，也用于 supervisor 的控制 Context。Node 的程序初值不能使 supervisor 和 child 无意进入不同网络；CLI 显式网络覆盖仍在第 6 层。不监控 readiness 的单进程 run 可以省略 context；这时运行器不注入默认值，应用正常按 program/file/environment 解析，也不报告 ready。

prepare 使用 core 解析器分别计算 child 与 supervisor 的最终 `domain_id/zenoh_config`；需要探针时，两者不一致就拒绝计划。显式的 launch `zenoh_config: null` 会覆盖较低层的环境配置路径，使两者共同使用 adapter 的默认配置。

```json
{
  "schema_version": 1,
  "context": {"domain_id": 0, "zenoh_config": "/opt/aos/config/client.json5"},
  "processes": [
    {
      "id": "provider",
      "package": "aos_demo",
      "entry": "worker",
      "args": [],
      "env": {},
      "config_files": [],
      "node": {"nodes": {"worker": {"namespace": "/demo", "parameters": {"limit": 3}}}},
      "depends_on": [],
      "readiness": {
        "node": "/demo/worker",
        "timeout": 10.0,
        "endpoints": [{"kind": "service", "name": "/demo/work", "interface": {"package": "aos_demo", "resource": "work"}}],
        "lifecycle": {"target": "active", "autostart": true}
      },
      "restart": {"policy": "on_failure", "delay": 1.0, "max_restarts": 3}
    },
    {
      "id": "consumer",
      "package": "aos_demo",
      "entry": "consumer",
      "args": [],
      "env": {},
      "config_files": [],
      "node": {"nodes": {"consumer": {"namespace": "/demo"}}},
      "depends_on": ["provider"],
      "readiness": {"node": "/demo/consumer", "timeout": 10.0},
      "restart": {"policy": "never"}
    }
  ],
  "shutdown": {"sigint_timeout": 5.0, "sigterm_timeout": 5.0}
}
```

endpoint 的 `interface` 从已安装资源解析，计划中固定实际 type_name/type_hash；kind 不一致在启动前拒绝。CLI 显式覆盖另传 `RunOverrides/LaunchOverrides`，不把文件内值误标为 cli；以 process id 选择目标，可覆盖已公开的 context/node 字段。每个来源中重复赋值拒绝。`env` 是 child 的显式 OS 环境增量；`AOS_PROCESS_CONFIG` 等运行器保留变量不能覆盖。

`args` 原样作为 argv 元素；不拆分字符串，不经过 shell。`cwd` 若提供须为绝对目录；未提供时固定为包的 `share/<package>`。业务资源通过索引获得绝对路径，不能要求调用者先 cd 到仓库。

macOS 的启动器和 script hook 使用直接 `execve`：没有 shebang、也未显式声明解释器的文本文件返回启动错误，不隐式交给 `/bin/sh`。嵌入 Rust 运行层的宿主必须保留子进程退出状态；若设置 `SIGCHLD=SIG_IGN` 或 `SA_NOCLDWAIT`，启动器在创建进程前返回错误。CLI 已在启动时恢复 `SIGCHLD` 默认处置。

## Rust runtime 的最小接口

```rust
pub struct PackageIndex { /* 显式固定的 canonical prefixes */ }
impl PackageIndex {
    pub fn discover(prefixes: &[PathBuf]) -> LaunchResult<Self>;
    pub fn package(&self, name: &str, version: Option<&str>) -> LaunchResult<InstalledPackage>;
    pub fn entry(&self, package: &str, entry: &str) -> LaunchResult<ResolvedEntry>;
    pub fn resource(&self, package: &str, name: &str) -> LaunchResult<ResolvedResource>;
}

pub fn parse_launch(text: &str) -> LaunchResult<LaunchSpec>;
pub fn prepare(index: &PackageIndex, spec: LaunchSpec, overrides: LaunchOverrides)
    -> LaunchResult<LaunchPlan>;

pub trait ReadinessProbe: Send {
    fn poll(&mut self, expected: &ProcessIdentity, requirement: &ResolvedReadiness,
            remaining: Duration) -> LaunchResult<ReadinessProgress>;
    fn forget(&mut self, expected: &ProcessIdentity);
    fn close(&mut self);
}
pub trait ProbeFactory {
    fn create(&mut self, context: &ResolvedLaunchContext) -> LaunchResult<Box<dyn ReadinessProbe>>;
}
pub trait EventSink {
    fn emit(&mut self, event: &LaunchEvent) -> std::io::Result<()>;
    fn log(&mut self, record: &ProcessLog) -> std::io::Result<()>;
}

pub struct Supervisor { /* 本次启动的 Child、process group、timer、pipe reader */ }
impl Supervisor {
    pub fn start(plan: LaunchPlan, probes: Option<&mut dyn ProbeFactory>) -> LaunchResult<Self>;
    pub fn poll(&mut self, max_wait: Duration, sink: &mut dyn EventSink) -> LaunchResult<RunProgress>;
    pub fn request_stop(&mut self, reason: StopReason);
    pub fn summary(&self) -> RunSummary;
}
```

runtime 不创建全局 daemon，不在库内安装全局信号处理器；CLI 把 Unix 的 SIGINT/SIGTERM/SIGHUP/SIGQUIT 或 Windows 的控制台中断转成 `request_stop`。`start` 只初始化运行目录和所需 probe，首次 `poll` 才启动 child。`poll` 的主动等待最多 10 ms，每轮输出读取至多 1 MiB、每个进程至多 64 KiB；sink 和 probe 的方法也必须有界、不阻塞。`Drop` 先关闭 probe；CLI 的 probe 为该操作提供最多 1 秒预算。之后在同一个 2 秒兜底预算内终止全部已拥有的组并回收 leader，不打印成功摘要；正常退出应显式驱动到 `Finished`。

workspace 已包含 `aos-launch`，依赖 `aos-core/serde/serde_json/uuid`，以及 Unix 上固定的 `nix = 0.31.3`，开启 `process/signal/fs`。Linux 使用 nix `waitid(WNOWAIT)` 与 `/proc`；macOS 的 nix 尚未提供 waitid wrapper，因此在私有 OS 模块内封装 libc waitid 和 libproc。Windows 使用 `windows-sys` 封装 Job Object、进程句柄和非阻塞 named pipe，在 `CreateProcessW` 时原子加入 Job，关闭最后一个 Job 句柄会终止后代。该实现不通过扫描 PID 发送终止请求。

运行期间的启动、就绪或 child 失败会先进入整组关闭。调用者应继续 `poll` 到 `Finished`，再检查 `summary().outcome/stop_reason/cleanup_errors`；不能把 `poll` 未返回 Err 当作运行成功。stdout sink 暂时返回 `WouldBlock` 时保留事件，暂停读取 child 输出，同时继续进程控制；sink 必须在自己的有限停滞期限后返回实际错误。永久输出失败会停止后续写入并继续回收，直到清理结束才返回 `output_io_error`。stderr 日志写失败按行计数，不触发业务进程关闭。

`ReadinessProbe` 隔离 transport/控制实现：CLI 使用一个共享控制 Context、持续 Graph watch 和生命周期服务。`poll` 只推进状态与取已完成结果，网络服务调用放在有界控制 worker，不阻塞 supervisor 的 stop/exit。等待使用一个 steady 总预算，每次子操作消耗剩余时间，不按重试重新开始期限。

## 就绪、重启和关闭语义

在启动任何进程前检查包/入口/资源、JSON、依赖 DAG、重复 id/FQN、超时/上限和所需接口。每条 `depends_on` 必须指向定义了 readiness 的进程，不能悄悄退化为“已经 fork”。只要准备失败，本次不得已有 child。

每个进程按以下可观察状态推进：

```text
pending → starting → started → discovered → [configuring → activating] → ready
                                        ↘ failed
ready/started → exited → [backoff → starting(new generation)] / finished / failed
any live state → stopping → exited
```

- `started` 只说明 OS 已创建 child；`discovered` 说明找到了归属匹配的 NodeCard；`ready` 要求所列 endpoint 的 name/kind/type_name/type_hash 以及可选 lifecycle 状态全部满足。
- 每次就绪证明绑定 launch/process/generation/PID 和 Node ID。出现同 FQN 多个活跃 Node 时明确报告歧义；不使用第一条响应猜 provider。
- Unix 上 `aos.process.v1` 要求 Node 位于 supervisor 启动的 leader 进程；包装脚本必须 `exec` 实际入口。Windows 允许 Python venv launcher 的 worker，但 NodeCard 的 launch/process/generation 必须匹配，且实际 PID 必须仍属于该代 Job。两者都不接受仅凭同名 Node 的就绪证明。
- `autostart=true` 只允许受管 Node 的 `unconfigured→configure→inactive→activate→active`。依次检查真实转换结果，收到 `success=false` 立即失败；超时后只查询当前状态，不重复发送不确定是否执行过的转换。已有 active 可以直接满足，其他非预期状态明确失败。
- 所有新启动的依赖方必须等 provider **当前进程代次已经取得** `ready`。这是每代一次的启动门槛：ready 后不持续检查 lifecycle 或进程内 Node 重建；依赖方自身重启也使用该代已有证明。它不表示持续健康或自动重新激活。provider 退出时清除其本代证明，尚未启动的依赖方继续等待。已经运行的消费者保持运行，发送 `dependency_unavailable`；不由 supervisor 隐式重放消费者业务或重启消费者。
- provider 有限重启后必须重新完成发现、所有 readiness 条件及显式 autostart；产生 `dependency_ready` 后才放行尚未启动的依赖方。重启生成新的 generation，旧 Node/结果不能满足新证明。
- 部分启动失败、就绪失败、非重启的失败退出或重启额度耗尽，默认停止整组并返回失败；不能留下已成功启动的 provider。自然零退出的单次任务标记 completed；若它仍被未启动的进程依赖且未完成 readiness，则视作启动失败。

重启规则如下：

| 配置/事件 | 行为 |
|---|---|
| 未给 restart / `never` | 不重启 |
| `on_failure` | 仅退出码非零或 signal 退出；spawn/配置错误不反复重试 |
| `always` | 可选支持：所有自然退出，包括零退出；仍受有限上限约束 |
| delay | 默认 1 秒，0..86400 steady 秒；等待期间收到停止立即取消 |
| max_restarts | 默认 3，非负整数；是初次启动之后额外次数，永不按 ready 重置；拒绝负数和无限 |
| 人为 stop、组失败清理、依赖清理 | 禁止所有重启 |

Unix 上每个入口启动到独立的 process group，记录 PID/PGID 与 generation，只给这些组发送信号。关闭按依赖逆序推进，同层并行发送 SIGINT→SIGTERM→SIGKILL。`sigint_timeout + sigterm_timeout` 是**整个 launch 的总宽限预算**，默认共 10 秒；每层按剩余层数均分尚余预算，INT/TERM 比例沿用配置。提前退出会把剩余时间留给后续层。总 deadline 到达后，全部尚未结束的组收到 SIGKILL，随后共享至多 2 秒回收预算，不能按进程数或层数倍增等待。

Windows 为每代入口创建独立 Job，并使用同样的逆依赖关闭顺序和总时间预算。两个宽限阶段发送 Ctrl-Break，最后终止 Job；`sigint_timeout`、`sigterm_timeout` 保留为跨平台配置字段名，不表示 Windows 存在 SIGTERM。非阻塞 named pipe 在每轮预算内读取，Job 结束后的残留字节通过 `PeekNamedPipe` 取得。

单个 readiness、重启 delay 和合计 shutdown 宽限都不得超过 86400 秒。prepare 拒绝超限值；运行时所有 deadline 加法均检查溢出，事件中的毫秒使用有界 u64，不因超大请求 panic。

Unix 上 leader 自然退出时立即终止仍存活的同组后代，避免一次任务退出后留下后台进程。通过 `WNOWAIT` 先观察退出但保留 leader；仅在同组无活进程、停止信号阶段结束后才回收。检查会区分活进程和已退出的僵尸；孙进程僵尸由其收养父进程回收，本运行器仍必须回收自己的 leader。读取或权限错误，以及到期仍存活的组，明确记录 `cleanup_failed`。回收 leader 后永久停止向其旧 PGID 发信号，防止数值被复用后误杀其他进程。

组回收与输出排空分别计时。leader 回收时，用 `FIONREAD` 为每个流记录当时内核可读字节，加上读取器尚未解析的本地字节。这份有限残留可以按每轮字节和行数配额逐步排空；进程较多时，轮转等待也不会把合法残留误报为清理失败。该额度只减不增，回收后的新写入不能补充它，连续恰好写满每轮行数配额也不能延长等待。

自然退出的残留排空有固定工作量，但没有独立的绝对时间期限。在 sink 持续接收、调用者约每 10 ms 推进一次且每轮访问 16 个进程槽位的条件下，最慢的单字节空行约需 `ceil(残留字节数 / 256) × 10 ms × ceil(槽位数 / 16)`；其他行形态还受每流每轮 32 KiB 的读取额度约束。Linux 管道若被扩容到 1 MiB，单槽空行残留可需约 41 秒，128 槽约为其 8 倍。该估算不含 sink 背压和调度暂停；调用者推进速度决定实际墙钟耗时。停止请求另受下文的绝对期限约束。

残留排完并检查过尾部后，每个进程还有一个 2 秒管道关闭窗口。新字节不延长这个窗口，因此组外写者即使低速持续写入，也不能无限保留管道。sink 返回 `WouldBlock` 或仍有待交付事件时会更新窗口，由 sink 的有限停滞期限负责；sink 恢复接收、事件交付完成且有限残留排空后，管道仍未在 2 秒内结束则报告 `cleanup_failed`。

leader 回收时，其所属组已经没有活进程。此后 stdout/stderr 各自新增读取的原始字节不能超过管道容量加 4 KiB：Linux 查询 `F_GETPIPE_SZ`，macOS 使用保守的 1 MiB 容量上限。超过该值说明仍有组外写者，立即关闭读端、报告 `cleanup_failed` 并停止本次运行，不向已经回收的 PGID 或组外进程发送信号。该额度按实际读取字节计算，超长行也消耗额度，与下游输出速度无关。

收到停止请求后还有一个不随输出进展延长的绝对期限：INT/TERM 总宽限加 2 秒组回收预算，再加 30 秒输出排空预算。到期仍有待交付事件或已回收进程的管道未关闭时，丢弃剩余输出，返回 `output_io_error`；已有输出错误不会跳过这一步，仍关闭已回收进程的读端并保留原错误。不把持续慢速读取归类为进程组清理失败。正常运行期间，sink 的 30 秒无进展策略仍独立适用；持续有进展的读取不会触发该停滞条件。

若进程已经全部结束，但最后的事件仍被背压，随后收到的停止请求也会为剩余输出建立 30 秒绝对期限；不会因为进程已结束而忽略该请求，也不会重写已经完成的进程结果。

Unix 进程主动 `setsid` 或转移到别的组会脱离普通 process group 归属，首期不承诺由运行库追踪逃逸进程；安装入口不得 daemonize。交互式运行时，supervisor 被 SIGKILL 或 OOM 终止无法执行清理，子进程可能继续运行。Linux [systemd 部署](system-service.md)使用 `KillMode=mixed`：先让 supervisor 处理 SIGTERM，截止时由 systemd 清理整个 service cgroup，也为 supervisor 崩溃提供兜底。Windows 的 Job 关闭规则负责其自有后代清理；Windows 服务注册另属系统集成。跨主机进程控制不在当前运行器的范围。

OS 管理器只管理 supervisor。supervisor 的 on_failure 负责业务 child；不得再给相同 child 单独套 OS 重启。systemd 配置、退出登录保活和进程清理方法见[系统服务](system-service.md)。

## 输出与有限资源

runtime 事件包含 `schema_version/launch_id/process_id/generation/event/elapsed_ms`，在已知时附 PID/PGID、退出码或信号、阶段和错误 code。至少覆盖 `process_started/discovered/lifecycle_transition/ready/process_exited/restart_scheduled/restart_exhausted/dependency_unavailable/dependency_ready/process_stopping/launch_finished`。

- CLI stdout 只写 supervisor JSONL。child stdout 经独立管道读取，每一条完整、无重复键的 JSON 值包装成 `process_stdout` 事件，原值位于 `details.value`，来源来自事件身份。大整数、高精度小数和指数保留原始 JSON 字节；Rust `LaunchEvent.details` 为 `RawValue`，sink 必须直接序列化事件，不能经 `Value` 中转。普通文本和带重复键的 JSON 交给 `EventSink::log` 写到带来源的 stderr 日志。child 无法通过自身 JSON 覆盖外层事件身份。
- child stderr 和运行器日志只到 stderr；业务 JSON payload 不复制到 telemetry。诊断不打印完整环境、凭证或启动描述。
- stdout/stderr 单行限制 64 KiB，每轮分别处理最多 256 行，并各自保留至多 4 KiB 尚未处理的读取尾部。达到行预算就暂停读取，由管道施加背压，不丢弃普通短行；超长行发 `output_truncated`，诊断日志写失败发有计数的 `output_dropped`。进程控制事件与输出队列分开，日志洪峰不能阻塞 stop/exit。
- stdout 与 stderr 必须由下游分别接收。`2>&1` 合并到同一管道时，两条写入线程可能在超过原子写入长度的行中间交错，合并结果不保证仍是合法 JSONL。
- stdout 写失败时进入整组关闭，回收后返回 `output_io_error`；不能把 child 留在后台。最终摘要区分正常完成、人为停止、启动/就绪/运行失败，并列实际重启数和清理失败。

## 实现入口与测试

public API 以 [types.rs](https://github.com/Cmoon-cyl/aos/blob/v0.2.0/crates/aos-launch/src/types.rs)、[plan.rs](https://github.com/Cmoon-cyl/aos/blob/v0.2.0/crates/aos-launch/src/plan.rs)、[supervisor.rs](https://github.com/Cmoon-cyl/aos/blob/v0.2.0/crates/aos-launch/src/supervisor.rs) 为接线入口。`ProcessBootstrap/DeploymentLayers/ProcessIdentity` 在运行层和 core 使用同一 v1 wire 契约；已安装 SDK 的专项测试会独立验证输入被标准 Context/Node 消费。

用 `cargo test -p aos-launch --test runtime` 执行本运行层验收。测试自建隔离安装前缀，并将测试程序安装为真实入口；一个 ignored helper 专供这些测试作为子进程启动。覆盖严格 JSON/版本/ABI/路径、预检零启动、argv/cwd/权限、依赖等待、代际刷新与消费者不重放、有限重启、部分启动失败、孙进程、整体停止 deadline、输出洪泛和写失败。单进程和 64 进程的满管道短行测试检查合法残留全部交付；8 种组外写者变体覆盖大行或连续 256 个空行、stdout/stderr、自然退出/停止的全部组合，另有每 200 ms 写短行和 sink 写失败的测试。慢速 sink 测试验证绝对停止期限。该测试入口不需要网络、router 或已接入 bootstrap 的 SDK。
