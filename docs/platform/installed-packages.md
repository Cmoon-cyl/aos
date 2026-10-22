# 安装包与 Python 资源查找

Python SDK 和 CLI 共用 Rust `aos_launch::PackageIndex`。查找只读取安装索引和文件元数据，不执行包入口，不导入业务模块，也不打开 Zenoh 会话。

```python
from aos_sdk import Interface, PackageIndex, get_package_resource

index = PackageIndex(["/opt/aos"])
package = index.package("my_robot", version="1.0.0")
resource = index.resource("my_robot", "command")
interface = Interface.load(resource["path"])

# 简写返回 pathlib.Path，适合交给 Interface.load 或应用自己的文件读取函数。
path = get_package_resource("my_robot", "command", prefixes=["/opt/aos"])
```

安装布局、manifest 格式和路径限制见 [launch 运行契约](launch.md#安装布局与发现)。`PackageIndex` 构造时固定 manifest 快照；安装或更新包后应重新构造。每次解析资源仍会检查文件存在且 canonical 路径留在安装前缀内。

| API | 返回值 |
|---|---|
| `PackageIndex(prefixes=None)` | 读取指定前缀的索引；显式传入空列表表示不搜索任何前缀 |
| `index.packages()` | 包名到候选包列表的字典，保留全部候选 |
| `index.package(name, version=None)` | 唯一命中的包，含 `prefix`、`manifest_path`、`manifest` |
| `index.resource(package, name, package_version=None)` | `package`、`name`、资源 `kind` 和 canonical 绝对 `path` |
| `get_package_prefix(package, prefixes=None, version=None)` | 包前缀 `Path` |
| `get_package_share_directory(package, prefixes=None, version=None)` | `share/<package>` 的绝对 `Path` |
| `get_package_resource(package, resource, prefixes=None, package_version=None)` | 声明资源的 canonical 绝对 `Path` |

未传 `prefixes` 时，普通 Python 进程读取 `AOS_PREFIX_PATH`，再加入当前已安装解释器的 `sys.prefix`；带 `AOS_PROCESS_CONFIG` 的 launch child 只使用 supervisor 固定的环境前缀，避免混入另一个解释器安装目录。路径列表使用当前平台的 `os.pathsep`，例如 Unix 为 `:`，Windows 为 `;`。整串空环境值视为未设置，非空列表中的空项报错。显式列表只使用指定前缀；cwd 从不自动参与搜索。调用者提供的相对路径按调用时 cwd 固定，最终必须为已存在的目录，同一 canonical 前缀去重。

同名包出现在不同前缀时，单包或资源解析报告 `ambiguous_package`。`version` 只是对唯一候选进行精确检查，不负责在多个版本中挑选包；使用显式前缀消除歧义。

原生错误通过 `AOSError` 传递，保留 `code` 和 `stage` 属性。例如：

```python
from aos_sdk import AOSError, PackageIndex

try:
    PackageIndex(["/opt/aos"]).resource("my_robot", "command")
except AOSError as error:
    print(error.code, error.stage, str(error))
```

常见错误包括 `package_not_found`、`resource_not_found`、`ambiguous_package`、`version_mismatch`、`invalid_manifest` 和 `invalid_argument`。读文件的系统错误为 `io`。调用方应保留具体错误，不将缺包或错误版本静默换成 cwd 中的文件。

## 安装示例包

仓库内的 `scripts/install_demo_package.py` 使用**已安装候选 wheel 的 Python**，把示例装入一个新前缀。它复制两份程序和接口资源，创建指向该解释器的入口，最后原子写入索引。再次使用同一前缀会拒绝覆盖。这个脚本供示例安装使用，不代表任意 pip 包会自动注册到 AOS 索引。

```sh
/path/to/installed-env/bin/python -I scripts/install_demo_package.py \
  --prefix /absolute/new/demo-prefix \
  --config /absolute/config/client.json5 \
  --domain-id 73 \
  --namespace /demo
```

Windows 安装器在 manifest 中声明包内 `.py`、绝对 `interpreter` 和 `interpreter_args: ["-I", "-u"]`，不依赖 shell 或文件关联。Unix 安装保留使用 `exec` 的可执行入口。

包内包含 lifecycle provider、普通 consumer，以及名为 `pair` 的 launch 资源。provider 以参数 `threshold` 决定收到多少条标签后报告 `ready`；consumer 只提交一次标签批次，随后观察状态。provider 重启后，consumer 不会自动重发业务消息。

两个示例都通过标准 `Context()` 和 Node 初始化读取部署配置，并通过 SDK 索引读取接口。配置自动注入、真实发现、lifecycle autostart、依赖启动和重启后的行为需要在完整 run/launch 集成中验证；资源查询及安装测试不替代这些验收。

`scripts/install_memory_package.py` 安装的 `aos_memory` 包另含只读入口 `request_observer` 和固定接口资源 `request_telemetry`，与 provider/consumer 一起使用绝对解释器、安装目录和 package index。`--export-request-telemetry` 才会在生成的 provider/consumer 请求中启用真实 Core capture；默认不启用。新增模块和 Interface 都在 `package_mvp.py` 的必需安装资源清单中，缺失时拒绝归档。安装路径、诊断权限和实时/回放用法见 [请求诊断说明](request-observer.md)。

`doctor_report` 创建独立诊断子进程，只验证其显式选择的 Zenoh 配置和 domain。该子进程排除 `AOS_PROCESS_CONFIG`，不继承业务进程的归属身份，也不合并 launch 的 CLI 覆盖；父进程环境保持原样。报告的 `deployment.process_config_present` 标明原环境是否有启动描述，`deployment.validation=not_run` 明确表示未验证该部署文件。诊断成功不能证明业务进程的 bootstrap 或有效配置正确。
