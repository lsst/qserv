@AGENTS.md

# Claude Code specifics

- Prefer the project skills (`build-qserv`, `itest`, `deploy-qserv`) over
  reconstructing those workflows from scratch; `deploy-qserv` carries the mandatory
  PVC safety checklist.
- When exploring large subsystems (`src/replica/` especially), delegate broad reads to
  subagents to protect context; the architecture docs in `doc/architecture/` usually
  answer structural questions without a code sweep.
