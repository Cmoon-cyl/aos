---
name: aos-develop
description: Build and verify Python nodes, services, topics and actions on AOS, using the installed SDK and interface contracts.
---

# Develop with AOS

First ensure the environment using ../aos/SKILL.md. Read
../aos/references/developing.md and the installed SDK documentation relevant to
the requested component. Resolve these paths relative to this skill file.

Use the installation's Python and one explicit connection configuration for the
provider and caller. Verify discovery and an actual business result. For Actions,
verify feedback and terminal result; include cancellation when it is part of the
requested behavior. Keep received, accepted and completed states separate.
