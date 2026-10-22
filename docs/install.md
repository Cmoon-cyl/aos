# 安装 AOS

使用完整平台包需要匹配架构的 **CPython 3.11**，不需要本机编译 Rust。
Windows 还需要 [Microsoft Visual C++ v14 x64 运行库](https://learn.microsoft.com/en-us/cpp/windows/latest-supported-vc-redist)。
原生 CLI、SDK 扩展、官方 Router 及其插件依赖 `VCRUNTIME140.dll`；请安装微软提供的 x64 运行库后，再执行下方命令。
本次 Windows 原生验证的运行库版本为 `14.51.36247.0`。
从 [GitHub Releases](https://github.com/Cmoon-cyl/aos/releases) 下载平台归档及同名 `.sha256`，
实际验证环境与兼容性边界见[支持范围](support.md)和[发行说明](releases/v0.2.0.md)。

## 选择平台

| 平台包 | 适用环境 |
| --- | --- |
| `aos-0.2.0-macos-arm64-<source>.tar.gz` | Apple Silicon macOS、arm64 CPython 3.11 |
| `aos-0.2.0-linux-x86_64-<source>.tar.gz` | x86_64 GNU/Linux、x86_64 CPython 3.11 |
| `aos-0.2.0-windows-x86_64-<source>.zip` | 64 位 Windows、x64 CPython 3.11 |

保持 CLI、wheel 和配置来自同一发行包。`<source>` 是验收源码 SHA 的前 12 位，
完整构建、验收和打包来源写在 `manifest.json` 中。

## macOS / Linux

把 `archive` 改为下载文件的实际路径，`.sha256` 放在同一目录：

```sh
archive=/absolute/path/to/aos-0.2.0-macos-arm64-SOURCE.tar.gz
python3.11 -c 'import hashlib,pathlib,sys; p=pathlib.Path(sys.argv[1]); want=pathlib.Path(str(p)+".sha256").read_text().split()[0]; got=hashlib.file_digest(p.open("rb"),"sha256").hexdigest(); assert got == want, (got,want); print("SHA256 OK")' "$archive"
mkdir aos-install
tar -xzf "$archive" -C aos-install
cd aos-install/aos-*
python3.11 -m venv .venv
. .venv/bin/activate
python -m pip install --no-index --no-deps wheels/aos_sdk-*.whl
python -I -c 'import aos_sdk; import aos_sdk._native; print(aos_sdk.__version__, aos_sdk._native.__file__)'
./bin/aos --help
./router/zenohd --version
```

每一步成功后再继续。Router 应输出 `zenohd 1.10.1`，可能带 `v` 前缀。
安装 wheel 不从网络下载 Python 依赖。

Linux Python 缺少 `ensurepip` 且本机已有 `uv` 时，可用以下命令替代创建 venv 和安装步骤：

```sh
uv venv --offline --python python3.11 .venv
uv pip install --offline --no-index --no-deps --python .venv/bin/python wheels/aos_sdk-*.whl
```

## Windows PowerShell

把 `$Archive` 改为实际 ZIP 路径。建议使用尚不存在的短解压目录（如 `C:\AOS\0.2.0`），
避免历史证据目录在深层路径下超过 Windows 工具的路径长度限制：

```powershell
$ErrorActionPreference = 'Stop'
$Archive = (Resolve-Path 'C:\path\to\aos-0.2.0-windows-x86_64-SOURCE.zip').Path
$Expected = ((Get-Content "$Archive.sha256" -Raw).Trim() -split '\s+')[0]
if ((Get-FileHash -Algorithm SHA256 $Archive).Hash.ToLowerInvariant() -ne $Expected) {
    throw 'AOS archive SHA256 mismatch'
}
$Destination = 'C:\AOS\0.2.0'
if (Test-Path $Destination) { throw 'Choose a new extraction directory' }
Expand-Archive -Path $Archive -DestinationPath $Destination
$Package = @(Get-ChildItem $Destination -Directory -Filter 'aos-*')
if ($Package.Count -ne 1) { throw 'Expected exactly one extracted AOS directory' }
Set-Location $Package[0].FullName
py -3.11 -c "import platform,sys; assert platform.machine().lower() in ('amd64','x86_64') and sys.maxsize > 2**32"
if ($LASTEXITCODE -ne 0) { throw 'Use x64 CPython 3.11' }
py -3.11 -m venv .venv
if ($LASTEXITCODE -ne 0) { throw 'Virtual environment creation failed' }
$Python = (Resolve-Path '.\.venv\Scripts\python.exe').Path
$Wheel = @(Get-ChildItem .\wheels\aos_sdk-*.whl)
if ($Wheel.Count -ne 1) { throw 'Expected exactly one SDK wheel' }
& $Python -m pip install --no-index --no-deps $Wheel[0].FullName
if ($LASTEXITCODE -ne 0) { throw 'SDK installation failed' }
& $Python -I -c "import aos_sdk; import aos_sdk._native; print(aos_sdk.__version__, aos_sdk._native.__file__)"
if ($LASTEXITCODE -ne 0) { throw 'Native SDK import failed' }
.\bin\aos.exe --help
if ($LASTEXITCODE -ne 0) { throw 'CLI could not start' }
.\router\zenohd.exe --version
if ($LASTEXITCODE -ne 0) { throw 'Router could not start' }
```

直接使用 venv 解释器即可，无需激活 PowerShell 脚本或修改执行策略。
保留整个 `router/` 目录，包括相邻 DLL 和 `deps/`。
若出现原生模块或 DLL 加载错误，按[故障排查](troubleshooting.md#python-无法导入)
记录具体错误并核对平台基线。

## 运行第一次业务

保持解压后的目录结构，在三个终端分别运行 Router、provider 和调用端。
[快速开始](getting-started.md)提供两种 shell 的完整命令，成功判据是实际 Service 响应 `sum: 42`。
能打印版本或 help 只证明程序能启动，不替代业务验证。

默认开发 Router 会监听网络接口，使用未认证的 TCP。
只需单机时显式选 loopback listener；跨不可信网络使用[mTLS 与每机 Router](platform/topology.md)。

## 包内目录

```text
aos-0.2.0-<platform>-<source>/
  bin/             原生 aos / aos.exe
  wheels/          当前平台 SDK wheel
  router/          固定 Zenoh 1.10.1 完整发行内容
  config/          开发 Router / session 配置
  interfaces/      示例 Interface JSON
  examples/        可运行的 Python 示例
  scripts/         demo 与 Memory 公开安装器
  docs/            使用与部署文档
  upstream/        来源、补丁和上游许可
  manifest.json    来源、验收状态和逐文件摘要
```

`.sha256` 用于校验传输完整性，不是发布签名。
旧诊断包的 manifest 若声明 `zenohd.bundled=false`，需要按
[`config/README.md`](../config/README.md)获取固定上游 Router；完整 0.2.0 发行包已经附带。

## 安装应用到独立前缀

公开安装器把程序入口、接口、launch 资源和索引写入一个尚不存在的 prefix，
入口固定使用安装时的绝对 Python。安装时不启动 Router 或 Node，不打开 Memory 业务数据。

```sh
# 使用刚安装 wheel 的 Python；路径替换为实际绝对路径。
python -I scripts/install_demo_package.py \
  --prefix /absolute/new-demo-prefix \
  --config /absolute/zenoh-session.json5 --domain-id 73 --namespace /demo

python -I scripts/install_memory_package.py \
  --prefix /absolute/new-memory-prefix --work-dir /absolute/owned-memory-data \
  --config /absolute/zenoh-session.json5 --domain-id 73 --namespace /memory
```

Windows 使用同样的 Python 参数与 Windows 绝对路径，可写成单行。
先确保所选 Router 已运行，再通过生成的 request 启动：

```sh
aos launch start --request /absolute/new-memory-prefix/share/aos_memory/requests/launch-pair.json
```

消费者输出 `demo_completed` 后 provider 继续运行；显式停止整组并检查清理摘要。
完整步骤和业务契约见 [MemoryServer](platform/memory-server.md)。

## 升级和卸载

升级使用新的解压目录和新 venv，保留旧版本、部署配置和业务数据。
开发期间多个候选共用 `0.2.0`，必须核对源码及文件摘要；
原地安装时需显式 `--force-reinstall`，不能把 pip 的“已安装”提示当作升级证据。

卸载前停止本次启动的应用与 Router；确认没有进程引用后删除对应安装目录和 venv。
Memory 工作目录、bag、证书和其他业务文件独立保留，不随程序目录自动清理。

维护者组包入口移至[源码构建与发行](development.md#组装完整平台包)。
