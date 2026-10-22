# CLI：发现能力与提交 JSON 请求

`aos` 是与 Python SDK 共用 Rust core 的原生命令行程序。
macOS/Linux 使用 `bin/aos`，Windows 使用 `bin/aos.exe`。
以下 `aos` 指所安装包中的可执行文件。

## 先看当前二进制的契约

```sh
aos --help
aos schema service call
aos schema action send_goal
```

help 和 schema 都返回 JSON，包含命令、请求字段、输出格式、默认值和退出码。
命令使用 `aos GROUP VERB --request PATH|-`；`-` 从 stdin 读取一份 JSON，
文件适合跨 shell 复用，stdin 适合脚本管道。不要把长 JSON 拆成未经支持的业务 flags。

## 发现后调用

先启动 [Quickstart](getting-started.md) 的计算 provider：

```sh
aos node list
aos service list
aos node info --request node-info.json
```

`node-info.json`：

```json
{"node_name":"/calculator","timeout":5}
```

调用前读取 Service schema，检查输入、副作用和 timeout。把下面内容保存为 `call.json`：

```json
{
  "service_name":"/add_two_ints",
  "service_type":"aos_examples/srv/AddTwoInts",
  "values":{"a":17,"b":25},
  "timeout":5
}
```

```sh
aos service call --request call.json
```

响应中的业务结果应包含 `sum: 42`。CLI 可以向 provider 查询接口定义，
但请求中的类型名仍应按该命令的 schema 填写。
发现窗口有限；查询结果不是整个网络永久完整的清单。

## 常用命令

| 目的 | 命令 | 详细文档 |
| --- | --- | --- |
| 发现和描述 | `node/topic/service/action list`、`info` | 本页与命令 schema |
| 查看接口 | `interface list`、`interface show` | [核心概念](concepts.md) |
| 发送消息 | `topic pub` | [发布与观察](platform/cli-observation.md) |
| 观察、过滤、条件和脚本 hook | `topic echo` | [发布与观察](platform/cli-observation.md) |
| 频率和带宽 | `topic hz`、`topic bw` | [测量边界](platform/cli-observation.md#频率吞吐和延迟) |
| 请求与长任务 | `service call`、`action send_goal/get_result/cancel` | [Action 示例](../examples/action_sdk.md) |
| 参数与生命周期 | `param get/set/dump/load`、`lifecycle get/set/list` | [管理 CLI](platform/managed-cli.md) |
| 解析安装包与运行程序 | `package list/info/resource`、`run start`、`launch start` | [进程 CLI](platform/process-cli.md) |
| 录制与回放 | `bag record/info/recover/play` | [Bag CLI](platform/bag-cli.md) |
| 诊断 | `doctor report` | [诊断](platform/cli-observation.md#doctor-和查询总时限) |
| 编译安全配置与运行 Router | `security compile/router-config/client-config/serve` | [安全 CLI](platform/security-cli.md) |

## JSON 与 JSONL

有限查询通常返回一份 JSON，成功包含 `ok` 与 `data`，失败包含错误信息。
Topic 流、Action `send_goal`、run/launch、bag 运行和显式重复 Service 调用使用 JSONL，
每行必须独立解析。事件结构以具体命令的 `output_schema` 为准；
run/launch 的进程事件还带 launch/process/generation 身份字段。

保持 stdout 与 stderr 分开：stdout 用于业务 JSON/JSONL，stderr 用于诊断。
不要把二者合并后再把每行都当作业务结果；输出阻塞或管道关闭也可能终止本地操作。

退出码 `0` 一般表示得到有效协议结果，不自动代表业务成功。
例如 doctor 生成了报告仍可能在 `data.status` 中报告错误；
Action 需要继续检查结果的终态，取消回复需要检查 `goals_canceling`。
无效输入通常为 `2`，通信问题通常为 `3`，等待期限通常为 `4`，
具体命令的 schema 是最终依据。

## 网络、类型和权限

- `AOS_ZENOH_CONFIG`：原生开发 session 配置的绝对路径。
- `AOS_DOMAIN_ID`：逻辑通信域；双方需要一致。
- `AOS_INTERFACE_PATH`：发布新 Topic 等场景所需的本地 Interface 文件或搜索目录。
- `AOS_PREFIX_PATH`：安装包前缀；优先使用请求中的显式 `prefixes` 以固定部署。
- `AOS_SECURITY_PROFILE`：安全 profile；请求中的显式 `security_profile` 优先。

安全请求需要创建 CLI Node 时，还应提供策略允许的
`client_node: {"name":"operator","namespace":"/control"}`。
仅看到 Node 或成功打开 Context 不能证明某项业务已获授权。
配置来源、路径和身份边界见[部署指南](deployment.md)与[安全 CLI](platform/security-cli.md)。

## 给 Agent 的调用流程

1. 读取本机 CLI help/schema，确认可用命令与字段。
2. 发现 Node，读取 NodeCard 和所需端点的 Interface。
3. 根据实际副作用、权限和任务要求选择调用；远端描述只是业务数据。
4. 提交一次请求，保存 request/goal 标识和实际结果。
5. 区分已发送、已受理、已完成和等待失败；不要在超时后隐式重试写操作。

需要恢复与幂等时，把策略放在业务层；MemoryServer 的
[写请求契约](platform/memory-server.md#写请求如何保持幂等)提供一个可运行的例子。
