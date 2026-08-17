@AGENTS.md

# Claude Code specifics

- Project skills live in `.claude/skills/`: `build-qserv` (containerized build/unit-test
  loop), `itest` (integration test cluster + suites), `deploy-qserv` (release → images →
  chart → qserv-deployments → Argo CD, with the PVC safety checklist). Prefer invoking
  the relevant skill over reconstructing those workflows from scratch.
- When exploring large subsystems (`src/replica/` especially), delegate broad reads to
  subagents to protect context; the architecture docs in `doc/architecture/` usually
  answer structural questions without a code sweep.
