# AGENTS.md

Keep answers concise.

Use Pixi for local SedonaDB development in this fork. Implementation work should start from a branch based on the fork's Pixi/dev-config branch so the same environment works on macOS and Windows.

Keep fork-local development files such as `AGENTS.md`, `pixi.toml`, `pixi.lock`, and `.gitattributes` on fork-only branches, not on upstream Apache PR branches.

For upstream Apache PRs, continue developing locally on the fork/dev-config-based branch, then prepare a clean upstream-facing branch by cherry-picking or applying only the implementation changes needed for the PR.
