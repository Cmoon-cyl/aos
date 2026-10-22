# AOS Agent Plugin

Install, update, discover and use AOS from Codex, Claude Code or TRAE CLI.
Includes aos-setup, aos, and aos-develop skills. All use the same installer.

Installing a plugin makes its skills available. Run its setup flow to prepare
the native AOS runtime; first-use checks also install missing dependencies.
The execution host needs network access. Downloads are public and checksum
verified. No GitHub login, system Python or Rust toolchain is required.

In Claude Code, install from the repository marketplace:

    /plugin install aos --marketplace Cmoon-cyl/aos

In Codex, use the repository marketplace or import the aos.plugin release asset,
then run the plugin's setup flow. In TRAE CLI, install a checked-out plugin:

    traecli plugin install ./plugins/aos

Plugin setup runs in the machine where the agent's terminal tools execute.

For a checked-out plugin:

    sh skills/aos/scripts/setup.sh --non-interactive --json

Windows uses skills/aos/scripts/setup.ps1 with the same arguments.
The result includes cli, python and verification. A passed verification means
an isolated local Service returned sum: 42.

The CLI manager provides env status/ensure/stop/test and update check/apply/rollback.
The native CLI remains available at native_cli in the installation result.
--version reports native runtime and manager versions separately.

See https://github.com/Cmoon-cyl/aos/blob/main/docs/for-agents.md and
https://github.com/Cmoon-cyl/aos/blob/main/docs/agent-installation.md for entry
points, update behavior and supported platforms.

AOS code is licensed under Apache-2.0. See LICENSE in this plugin.
