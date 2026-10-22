# 源码构建与发行

本页面向维护者。使用发行包不需要编译，安装步骤见[安装指南](install.md)。
仓库固定 Rust 1.99.1、PyO3 0.30.1、maturin 1.15.0、Zenoh/zenoh-ext 1.10.1。
保留 `Cargo.lock` 与两处 vendor patch，不用未打补丁的上游 wheel 替代本项目 SDK。

## macOS / Linux 构建

在干净源码检出根目录，准备本机原生工具链和 CPython 3.11：

```sh
rustup toolchain install 1.99.1 --profile minimal --component rustfmt --component clippy
python3.11 -m venv .venv
. .venv/bin/activate
python -m pip install maturin==1.15.0
export PYO3_PYTHON="$(command -v python)"
cargo build -p aos-cli --release --locked
python -m maturin build --release --locked --out dist
```

Linux 的原生产物构建再加 `--compatibility linux`；该选项不建立 manylinux 兼容性。
macOS arm64 应使用原生 arm64 Python 和 Rust，不通过 x86_64 转译环境混合构建。

## Windows 构建

需要 x64 CPython 3.11、MSVC 工具链以及依赖要求的 CMake。
在能够找到 MSVC 的 PowerShell 环境执行：

```powershell
rustup toolchain install 1.99.1 --profile minimal --component rustfmt --component clippy
py -3.11 -m venv .venv
$Python = (Resolve-Path '.\.venv\Scripts\python.exe').Path
& $Python -m pip install maturin==1.15.0
$env:PYO3_PYTHON = $Python
cargo build -p aos-cli --release --locked
& $Python -m maturin build --release --locked --target x86_64-pc-windows-msvc --out dist
```

每条命令都应检查退出码；不能在构建失败后使用旧 `target/` 或 `dist/` 中的文件冒充新产物。

## 本地验证

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
cargo test --workspace --doc --locked
python -I -m unittest discover -s scripts -p 'test_*.py' -v
```

安装后测试必须从源码树外、用新 venv 和安装的 wheel 执行。
`scripts/native_acceptance.py` 统一完成构建、上游摘要检查、原生测试、新环境安装、
真实进程与 CLI 业务验证，并保存原始日志和 provenance：

```sh
python3.11 -I -X utf8 scripts/native_acceptance.py --output /absolute/new-native-output
```

Windows 使用本机 CPython 3.11 和绝对新输出路径。脚本拒绝脏源码树，
校验首尾 commit 与产物摘要。测试报告分别记录发现数量、执行数量、失败和平台 N/A；
跳过不能计作通过。测试所需的默认 7447 端口应处于可供该次测试独占的状态。

可用 `AOS_BUILD_TARGET_DIR` 选择 Cargo 缓存；安装环境仍需是新环境。
固定上游下载可通过 `AOS_ZENOHD_ARCHIVE`、`AOS_ZENOH_CRATE_ARCHIVE`、
`AOS_ZENOH_TLS_CRATE_ARCHIVE` 指向已缓存文件，脚本仍核对固定摘要。

## 组装完整平台包

```sh
python3.11 scripts/package_mvp.py \
  --source-repo . \
  --artifacts /absolute/native-output \
  --output /absolute/new-release-assets \
  --platform macos-arm64 \
  --source-sha FULL_40_CHARACTER_COMMIT \
  --bundle-router
```

平台可选 `macos-arm64`、`linux-x86_64`、`windows-x86_64`。
Unix 生成 tar.gz，Windows 生成 ZIP，每个归档附带 `.sha256`。
完整发行使用 `--bundle-router`，从原生验收保留的官方 ZIP 重新提取全部 Router 文件，
核对固定平台摘要及实际验过的 executable；Windows DLL/deps 不能丢失。
未指定该参数只适用于已有外部 Router 的诊断包。

打包器校验 CLI/wheel 的路径、平台标签和摘要，以及安装器依赖和上游许可。
它不运行测试、不改写验收状态，非通过记录仍可生成诊断包，不能作为正式发行资产。

`manifest.json` 记录：

- `source_sha`：CLI/wheel 的实际构建来源。
- `validation_source_sha`：验收源码；归档名使用该 SHA 的前 12 位。
- `packaging_revision`、`packaging_tracked_changes`、`packaging_script_sha256`：打包输入。
- `acceptance_status`：构建输入的验证状态，不由打包步骤提升。
- `zenohd`：固定上游归档与可执行文件摘要。
- `files`：除 manifest 自身外的逐文件字节数与 SHA256。

原始测试日志和主机路径保存在仓库外的构建输出中，不进入公开发行包。
构建、验收和打包版本不同时，必须保留来源及未变输入的依据，不能只重标 SHA。

## 发行步骤

1. 固定同一候选，完成适用平台的原生测试和安装后业务。
2. 核对跨机、退出、恢复和本次声明的混合负载；保留实际候选与未覆盖范围。
3. 更新首页、使用文档、迁移说明、CHANGELOG 和发行说明。
4. 完成代码审查并合入；在干净源码上组包，解压到新目录验证安装与 Quickstart。
5. 发布对应 tag、三平台完整包、wheel/source distribution 与校验文件；回读下载摘要和资产清单。

发行说明列出功能、平台支持和已知限制；原始测试记录由维护者单独保管。
发布前必须实际解压安装并运行示例，不能用 CI 启动、ZIP 生成或 help 成功代替。
源码包构建与校验见[源码分发](platform/source-distribution.md)。
24 小时长稳和完整性能矩阵保留独立范围，不能把较短 smoke 写成这两项通过。
