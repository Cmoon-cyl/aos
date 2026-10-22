# 固定来源与 AOS 差异

下列固定版本用于说明 AOS 的行为来源与差异。Jazzy 行为参考与 rmw_zenoh/Zenoh 的独立快照分别列出；完整采用范围和许可证见 `upstream/source-map.json`。

## ROS 2 行为来源

| 仓库与固定 SHA | 直接读取的路径 | 采用机制与 AOS 差异 |
|---|---|---|
| rcl Jazzy `18d96c9c2b98e616324ae9a2c7701dd0b0886ff0` | [node.c](https://github.com/ros2/rcl/blob/18d96c9c2b98e616324ae9a2c7701dd0b0886ff0/rcl/src/rcl/node.c)、[remap.c](https://github.com/ros2/rcl/blob/18d96c9c2b98e616324ae9a2c7701dd0b0886ff0/rcl/src/rcl/remap.c) | Node 名先于 namespace，局部规则先于全局、首次匹配。AOS 使用自己的 JSON 配置层级与精确 remap 子集，未实现 ROS 完整语法 |
| 同上 | [timer.c](https://github.com/ros2/rcl/blob/18d96c9c2b98e616324ae9a2c7701dd0b0886ff0/rcl/src/rcl/timer.c) | 复用时钟跳变、周期推进/错过周期处理；AOS 额外固定所有控制等待使用 steady |
| 同上 | [default_state_machine.c](https://github.com/ros2/rcl/blob/18d96c9c2b98e616324ae9a2c7701dd0b0886ff0/rcl_lifecycle/src/default_state_machine.c)、[com_interface.c](https://github.com/ros2/rcl/blob/18d96c9c2b98e616324ae9a2c7701dd0b0886ff0/rcl_lifecycle/src/com_interface.c) | 生命周期状态、转换、私有服务与 transition_event；AOS 必须另实现受管实体 gate/drain，不能推断所有回调自动停止 |
| 同上 | [Action default_qos.h](https://github.com/ros2/rcl/blob/18d96c9c2b98e616324ae9a2c7701dd0b0886ff0/rcl_action/include/rcl_action/default_qos.h) | status KEEP_LAST/1/reliable/transient_local；AOS 继续使用 JSON 接口 |
| rmw Jazzy `8cb94a87cf3f4049abe2d563af893517bf336930` | [qos_profiles.h](https://github.com/ros2/rmw/blob/8cb94a87cf3f4049abe2d563af893517bf336930/rmw/include/rmw/qos_profiles.h) | default、sensor_data、services、parameters、parameter_events 的明确值；该快照 parameter_events 为 volatile。AOS 不开放其他 DDS QoS 字段 |
| rclpy Jazzy `4e607c09d624debc39a4ae5405680c06d87fac35` | [time_source.py](https://github.com/ros2/rclpy/blob/4e607c09d624debc39a4ae5405680c06d87fac35/rclpy/rclpy/time_source.py)、[timer.py](https://github.com/ros2/rclpy/blob/4e607c09d624debc39a4ae5405680c06d87fac35/rclpy/rclpy/timer.py) | use_sim_time、Clock/Timer/Rate；clock 订阅 depth1/best_effort。AOS 不引入 rclpy/ROS 运行依赖 |
| 同上 | [node.py](https://github.com/ros2/rclpy/blob/4e607c09d624debc39a4ae5405680c06d87fac35/rclpy/rclpy/node.py)、[parameter_service.py](https://github.com/ros2/rclpy/blob/4e607c09d624debc39a4ae5405680c06d87fac35/rclpy/rclpy/parameter_service.py) | 声明/类型/校验、原子 set 与事件；AOS 的部署覆盖优先级是明确新增契约，不声称与 rclpy 构造参数完全相同 |
| 同上 | [lifecycle/node.py](https://github.com/ros2/rclpy/blob/4e607c09d624debc39a4ae5405680c06d87fac35/rclpy/rclpy/lifecycle/node.py)、[publisher.py](https://github.com/ros2/rclpy/blob/4e607c09d624debc39a4ae5405680c06d87fac35/rclpy/rclpy/lifecycle/publisher.py) | managed entity 回调与 publisher gate；不是所有 Service/subscription 自动受管的证明 |
| rcl_interfaces Jazzy `7aa3caf43377ea6ad615bc1040832e2c7566bfbe` | [action_msgs](https://github.com/ros2/rcl_interfaces/tree/7aa3caf43377ea6ad615bc1040832e2c7566bfbe/action_msgs)、[lifecycle_msgs](https://github.com/ros2/rcl_interfaces/tree/7aa3caf43377ea6ad615bc1040832e2c7566bfbe/lifecycle_msgs) | 消息/状态/转换定义；AOS 沿用语义，以 JSON Schema 表达，不实现 CDR wire 互通 |
| launch Jazzy `41d58e36823026aa81955133c3b27d8baa596899` | [execute_local.py](https://github.com/ros2/launch/blob/41d58e36823026aa81955133c3b27d8baa596899/launch/launch/actions/execute_local.py) | 退出事件、respawn、SIGINT→SIGTERM→SIGKILL。AOS 默认不重启，显式重启默认有限 3 次；不搬入上游负数表示无限次的默认 |
| rosbag2 Jazzy `12b5e8ba4eb5665501a16f0d9e050552a1149f56` | [README](https://github.com/ros2/rosbag2/blob/12b5e8ba4eb5665501a16f0d9e050552a1149f56/README.md) | 成熟 record/play、split、QoS override、pause/rate/seek、clock。AOS 直接使用 MCAP JSON，seek 包含目标边界；不依赖 ROS storage plugin |
| rmw_zenoh 固定 rolling `dda52d6d9bc863b4b6c86a0b4fd1c6788f4a651e` | [rmw_client_data.cpp](https://github.com/ros2/rmw_zenoh/blob/dda52d6d9bc863b4b6c86a0b4fd1c6788f4a651e/rmw_zenoh_cpp/src/detail/rmw_client_data.cpp)、[rmw_service_data.cpp](https://github.com/ros2/rmw_zenoh/blob/dda52d6d9bc863b4b6c86a0b4fd1c6788f4a651e/rmw_zenoh_cpp/src/detail/rmw_service_data.cpp) | Action GetResult 使用协议最大 timeout；请求/回复 KEEP_LAST 队列满丢最旧。AOS 不引入业务自动重试，不把 depth 当执行并发上限 |


## Zenoh 身份与权限

Zenoh 版本 **1.10.1**，实际 HEAD `1211779c3647f5a96713dade452c546a07823580`。

| 源码 | 已核验事实 | AOS 要做的事 |
|---|---|---|
| [DEFAULT_CONFIG.json5](https://github.com/eclipse-zenoh/zenoh/blob/1211779c3647f5a96713dade452c546a07823580/DEFAULT_CONFIG.json5) | `cert_common_names`、TLS mTLS/名称校验/到期断连、ACL 消息种类；ZID 本身不带认证保证 | 显式安全 profile、证书/策略校验及正反例；不能继承默认 `enable_mtls=false` |
| [TLS unicast.rs](https://github.com/eclipse-zenoh/zenoh/blob/1211779c3647f5a96713dade452c546a07823580/io/zenoh-links/zenoh-link-tls/src/unicast.rs) `get_client_cert_common_name` | 提取对端 leaf 证书第一个可读 CN；无 CN 时为 None | 每个部署主体分配唯一 CN；缺失不授业务权限 |
| [access_control.rs](https://github.com/eclipse-zenoh/zenoh/blob/1211779c3647f5a96713dade452c546a07823580/zenoh/src/net/routing/interceptor/access_control.rs)、[authorization.rs](https://github.com/eclipse-zenoh/zenoh/blob/1211779c3647f5a96713dade452c546a07823580/zenoh/src/net/routing/interceptor/authorization.rs) | 从 transport link auth ID 形成 cert_common_name subject，并按规则匹配 | 只承诺受控 router 的资源/操作 ACL；这不证明 provider 已取得原始 caller |
| [query.rs](https://github.com/eclipse-zenoh/zenoh/blob/1211779c3647f5a96713dade452c546a07823580/zenoh/src/api/builders/query.rs)、[reply.rs](https://github.com/eclipse-zenoh/zenoh/blob/1211779c3647f5a96713dade452c546a07823580/zenoh/src/api/builders/reply.rs) | query 可设置拥塞控制；reply 的独立 congestion_control/priority 被标为无效果并继承 query | Service 本期只提供 reliable/volatile，其他取值结构化拒绝，避免 no-op 配置 |

证书映射与 Router 权限的使用方式见[安全文档](security.md)。

## MCAP 格式与上游 API

MCAP 固定 `93611496b526f29378ea987ab3e7ba0c67942cea`，该快照 Python 包内版本为 1.6.0。读取 [registry](https://github.com/foxglove/mcap/blob/93611496b526f29378ea987ab3e7ba0c67942cea/website/docs/spec/registry.md)、[writer.py](https://github.com/foxglove/mcap/blob/93611496b526f29378ea987ab3e7ba0c67942cea/python/mcap/mcap/writer.py)、[reader.py](https://github.com/foxglove/mcap/blob/93611496b526f29378ea987ab3e7ba0c67942cea/python/mcap/mcap/reader.py)。AOS 的文件格式和回放规则见 [bag 格式](bag-format.md)。
