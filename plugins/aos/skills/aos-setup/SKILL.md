---
name: aos-setup
description: Set up the AOS plugin by installing its compatible CLI, Router and Python SDK, verifying a real request, and reporting the usable command path.
---

# Set up AOS

Run the setup workflow in ../aos/SKILL.md using ../aos/scripts/setup.sh or
../aos/scripts/setup.ps1. Resolve these paths relative to this skill file.
Complete installation in the current execution host, parse the verification
result, and report the absolute CLI and Python paths.

If AOS is already installed, run env status and env test to confirm it is usable.
Do not upgrade an existing working installation merely because this setup skill
was loaded. If the installed commands do not satisfy the requested workflow,
explain the compatibility issue and use the user's existing upgrade authorization.

After a successful setup, continue any AOS task that prompted installation.
An installed plugin alone is not evidence that its runtime is ready.
