# 进程启动配置

标准 Rust 入口 `aos_zenoh::open_context(ContextConfigRequest)` 和 Python
`aos_sdk.Context()` 自动读取 `AOS_PROCESS_CONFIG` 指向的启动文件。Python
`Node`、`LifecycleNode` 自行创建 Context 时也走同一入口，应用不需要解析启动文件。

启动文件使用以下 v1 结构：

```json
{
  "schema_version": 1,
  "launch_id": "606c355e-00a9-405b-9f41-c5855426dfe2",
  "process_id": "worker",
  "generation": 1,
  "context": {
    "config_files": ["/opt/demo/context.json"],
    "launch": {"zenoh_config": "/opt/demo/peer.json5"},
    "cli": {"domain_id": 7}
  },
  "node": {
    "config_files": ["/opt/demo/node.json"],
    "launch": {"nodes": {"/app/worker": {"namespace": "/deployment"}}},
    "cli": {"nodes": {"/app/worker": {"parameters": {"rate": 6}}}}
  }
}
```

`context` 和 `node` 必须存在；其中 `config_files` 默认空列表，`launch`、`cli`
默认空对象。`launch_id` 必须是 UUID；`process_id` 为 1–128 个 ASCII 字母、数字、
下划线或连字符；`generation` 为 1 到 `u32::MAX`。文件上限为 1 MiB，必须是 UTF-8
JSON，重复键、未知字段、不支持的版本和错误类型均拒绝。定位路径、两处
`config_files`、非 null 的 Context `launch/cli.zenoh_config` 都必须是绝对路径。
不展开变量或执行文件内容。

每个标准 Context 在打开 Zenoh 会话前读取、校验并合并一次启动输入，保存不可变副本。
此后普通 Node 和 LifecycleNode 都使用该副本；修改文件或环境变量不会改变已有
Context 的启动输入。新 Context 会重新读取文件。节点仍按声明时的 name/FQN
选择规则，改名后的最终 FQN 不会改变选择条件。

配置继续按 `default → program → config files → environment → launch → cli`
解析。应用传入的配置文件先读，启动文件列出的文件后读，顺序和重复文件都保留。
应用与启动文件在同一 `launch` 或 `cli` 层可以提供不同字段；相同叶子字段重复提供
就拒绝，即使值相同。数组和 null 都视为完整叶子，不能在同层覆盖。不同层继续按
既有优先级覆盖，`effective_config` 保留 `file:<path>`、`environment`、`launch`
和 `cli` 来源。

Python 保留 `AOS_DOMAIN_ID`、`AOS_ZENOH_CONFIG` 以及 Node 名称、命名空间、
模拟时间的环境白名单。Rust 标准入口只自动读取启动文件定位变量，其他环境覆盖由
调用者显式放入 `ContextConfigRequest.environment`。启动文件定位变量本身不会
进入 environment 配置层。

有启动输入的 NodeCard 增加可选 `process`：

```json
{
  "launch_id": "606c355e-00a9-405b-9f41-c5855426dfe2",
  "process_id": "worker",
  "generation": 1,
  "pid": 12345
}
```

前三项来自校验后的启动文件，`pid` 取创建 Context 时的实际本机进程 ID。
这些字段用于启动实例关联，由进程自行报告，不能证明身份或存活。没有启动输入时
省略整个 `process` 字段。GraphEntity 的结构保持原有定义。

低层 `Context::new(transport)` 接收已经打开的传输，不读取启动文件。
`Context::from_configured_transport(transport, resolved, bootstrap)` 仅接收已经
解析的输入并保存副本，也不会重新打开或调整网络会话。调用这两个入口的适配器须
自己保证传输会话与配置一致。`Node::new_resolved*` 同样要求调用者先完成配置组合。

参数覆盖仍在应用声明参数时生效，不自动声明参数；原有类型、范围、只读和 validator
检查保留。Lifecycle 参数的 `allowed_states` 约束后续修改，启动声明沿用既有规则。

验证入口：

```text
cargo test -p aos-core --test process_bootstrap
cargo test -p aos-zenoh --test process_bootstrap
python -I /absolute/checkout/python/tests/test_platform_process_bootstrap.py -v
```

Python 检查要求先安装本次构建的 wheel，并从仓库外工作目录运行；测试使用真实
Zenoh peer，覆盖标准构造入口、Context 快照、多 Context 代际、声明身份选择、
来源保留、同层冲突及参数声明约束。它不替代 supervisor 的进程存活、readiness、
重启和停止验收。

2026-10-21 的 Mac arm64 开发候选已完成：

| 检查 | 结果 |
|---|---|
| aos-core 全部测试及 doctest | 105 通过，含 6 项 bootstrap 专项 |
| aos-zenoh bootstrap 集成 | 2 个真实子进程场景及 1 个 helper 通过 |
| PyO3 单元测试 | 10 通过 |
| 仓库外安装的开发 wheel | 7 项 bootstrap 与 32 项 foundation 测试通过 |
| Rust 格式与所涉三个 crate 的全部 targets Clippy | 通过，Clippy 使用 `-D warnings` |

此次 wheel 为 `cp311-abi3-macosx_11_0_arm64`，Python 3.11 从独立虚拟环境导入
`site-packages/aos_sdk/_native.abi3.so`。这些结果只说明本机开发候选的 SDK 接入，
Linux 原生安装和启动器完整验收由后续集成记录。
