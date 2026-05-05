---
name: Never run Python scripts without permission
description: User must run all Python scripts themselves in the terminal — never auto-execute
type: feedback
---

Never run Python (or other) scripts autonomously. Write and edit scripts, then wait for the user to run them in their terminal.

**Why:** User wants full control over when scripts execute, especially for scripts that modify files or the database.

**How to apply:** After writing or editing a script, stop and tell the user it's ready to run — do not call Bash to execute it.
