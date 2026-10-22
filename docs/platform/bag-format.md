# AOS bag 文件格式

0.2.0 使用下述已固定格式；当前原生验证与发行范围见[发行说明](../releases/v0.2.0.md)。运行接口与本机验证范围见[bag 共用运行层](bag-runtime.md)。选用 MCAP 容器；自定义 profile 为 `aos-json-v1`，不使用 `ros2` profile。MCAP registry 已定义 `message_encoding=json` 和 `Schema.encoding=jsonschema`；ROS 2 profile 则要求 CDR，不能把本方案称为 ROS wire/CDR 兼容。[固定 registry](https://github.com/foxglove/mcap/blob/93611496b526f29378ea987ab3e7ba0c67942cea/website/docs/spec/registry.md)

## 文件中的内容

| MCAP 记录 | AOS 内容 |
|---|---|
| Header | `profile=aos-json-v1`；library 记录 AOS/MCAP writer 版本 |
| Schema | name 使用 AOS message type 名；encoding 为 `jsonschema`；data 是 UTF-8 JSON Schema Draft 2020-12 的 **message 部分** |
| Channel | topic 是展开后的 AOS 名称；message_encoding 为 `json`；metadata 保存下面的 AOS 字段 |
| Message | data 为收到的原始 JSON payload 字节；不重新序列化业务消息；记录 sequence、log_time、publish_time |
| Metadata | `aos.recording` 保存 writer 版本、recording_id、时钟锚点与配置；值按 MCAP 要求编码为字符串。最终统计、每片 SHA256、结束原因写入 recording 的 `manifest.json` |
| Chunk/Index | 有界 chunk、消息和 chunk 索引；Schema/Channel 在首次使用前写入，支持脱离 provider 查询 |

以上容器字段由成熟 MCAP writer 生成，不自行重写 MCAP 编码器。Schema、Channel、Message 和索引的关系来自[固定 MCAP 规范](https://github.com/foxglove/mcap/blob/93611496b526f29378ea987ab3e7ba0c67942cea/website/docs/spec/index.md)；AOS 元数据及回放策略是本期新增契约。

Channel metadata 至少含：

```json
{
  "aos.format_version": "1",
  "aos.channel_id": "<immutable channel descriptor id>",
  "aos.type_name": "aos_examples/msg/String",
  "aos.type_hash": "<SHA256 of the complete RFC8785-normalized Interface>",
  "aos.interface": "<complete Interface encoded as a JSON string>",
  "aos.schema_part": "message",
  "aos.offered_qos": "<JSON array of observed publisher profiles>",
  "aos.record_qos": "<the recorder subscription's effective profile>",
  "aos.log_time_domain": "recording_anchored_steady",
  "aos.publish_time_source": "unavailable",
  "aos.sequence_epoch": "0",
  "aos.channel_role": "business",
  "aos.companion_for": "null",
  "aos.action_observation": "null"
}
```

完整 AOS Interface 包含 name/kind/message 等字段，本身不等于验证 payload 的 JSON Schema。其完整内容和现有 RFC 8785 + SHA256 算法都保留；回放前用 core 重算 hash，检查 Schema record 与 `interface.message` 一致，再校验 payload。不把手写的 JSON 排序算法作为通用 JCS 替代。

同名不同 hash、有效 QoS 或时间来源改变时新建 channel，不能覆盖旧 channel。多 publisher 的 offered QoS 是发现快照，不自动证明每条消息来自哪位 publisher；传输未提供可核验来源时该项保持 unknown。bag 不把 NodeCard 自述升级为认证身份。

全局录制序号为 u64：MCAP Message.sequence 保存低 32 位，`aos.sequence_epoch` 保存高 32 位；epoch 改变时换 channel。每个业务 Channel 都有独立 `sample_metadata` Channel，`aos.companion_for` 是业务 Channel id 的 JSON 字符串。配套消息保存收到的 adapter delivery/source 元数据及原 payload 字节数，自己的 topic 固定为 `/_aos_bag/sample_metadata`，仅用于文件内证据，不对网络回放。

每条业务消息及其配套 metadata 在同一 CRC chunk 内写入并成对验证。严格 inspect 拒绝孤立记录；recovery 分别计数并丢弃孤立业务/metadata，不能跨 chunk 用同序号拼接。Action 观察 Channel 的 `aos.action_observation` 保存 base、feedback/status kind 和完整 parent Action Interface；离线重建 derived Interface 并核对 hash 后才能回放。

## 时间与顺序

录制开始记录一次 system 时间锚点及 steady 起点。`log_time = system_anchor_ns + steady_elapsed_ns`，并明确标识 `recording_anchored_steady`；该字段给同一 recording 稳定排序，不保证系统校时后的真实 UTC。分片共用 recording_id、锚点和单调序列。

MCAP 的 publish_time 不可得时必须填 log_time；AOS 同时写 `aos.publish_time_source=unavailable`，以免下游把差值 0 当成零延迟。现有 adapter 的 delivery 时间和 Zenoh HLC 来源单独保存在配套 metadata 中；HLC 不直接解释为 Unix publish_time，也不从用户任意 `stamp` 字段推断。本期 writer 只写 `unavailable`；未来引入可核验发布时间时必须增加明确时间域契约并建立新 channel。

recorder sequence 表示本录制器已接收的顺序，不能用于证明发送端没有丢样。可用的 source sequence、adapter 队列丢样和 recorder 写队列丢样分别统计。无端到端序号时，上游丢样量为 unknown，不能报 0。

play 默认按 log_time、相同时间按文件内顺序重放，倍速必须有限且大于 0。暂停停止推进回放位置；可选 clock 可继续发布相同值。seek 以 recording 时间偏移定位，清空旧调度队列并发时钟跳变，再从目标处首条 `log_time >= target` 的消息继续。本期采用包含目标时刻的 seek；这与所固定 rosbag2 文档的“目标之后”表述有差异，须用边界样本测试。

使用独立回放 namespace，默认要求显式提供该 namespace；要回放到原名必须显式 opt-in。QoS 默认使用明确、可验证的记录 profile，多 publisher profile 有冲突时要求调用方选择或覆盖，不自动猜最优值。回放消息沿正常 SDK 校验、名称映射和 ACL；clock 同样需要发布权限。

## 录制、恢复和副作用边界

范围包括普通 Topic、Action feedback/status、参数事件和 lifecycle 事件。Service/Action 的诊断记录是单独的观察消息；play 只发布这些诊断消息，不调用 Service、提交 goal、取消 goal 或改参数。

录制器默认采用无压缩 chunk，保留 CRC 和索引；压缩未启用。默认 chunk 上限 4 MiB、最长 1 秒刷新；文件按 256 MiB 或 10 分钟切片。有效配置保存在 manifest 中，生产吞吐需要在实际部署环境测量；这些设置不代表 fsync 级零丢失。

目录记录清单、每片大小/SHA256、连续时间范围、是否 clean_close。磁盘不足、写入失败、总容量上限、用户中断要有不同结束原因。默认达到总容量后停止，不静默删除旧数据。内存写队列同时有条数和字节界限；溢出计数可见。损坏/截断片只能恢复完整校验通过的记录，另写恢复结果并标记 incomplete；不覆盖原文件，不把可读前缀称为完整录制。

## 实现与兼容性

Rust 实现使用 `mcap = 0.26.0`，关闭默认压缩特性。Python MCAP writer 不是运行依赖。
存储、恢复与回放接口见[运行层](bag-runtime.md)，命令用法见[bag CLI](bag-cli.md)。
