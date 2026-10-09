# Contributing

## Prerequisites

- Docker with Compose v2 (the integration tests use Testcontainers, so Docker must be running for `make test`).
- For work outside containers: Node.js 22, JDK 21 (the Maven wrapper in `services/` downloads Maven), Python 3.12
  with `ruff`.

## Day-to-day commands

```bash
make up          # build and start the stack, wait for health checks
make lint        # eslint/prettier, spotless, ruff
make test        # unit + integration tests for gateway, relay, consumer
make e2e         # full stack + dup30 load profile + invariant verification
make down
```

`make help` lists everything. Each component can also be driven directly:

```bash
cd services/gateway-node && npm test && npm run test:integration
cd services && ./mvnw verify -pl relay-java
cd scripts/python-bench && python run.py --profile dup30 --total 500
```

## What a change needs

- Tests at the level where the behaviour lives: pure logic gets a unit test, anything that touches SQL or Kafka gets a
  Testcontainers test, anything that changes delivery semantics gets an `e2e` run.
- An update to `docs/` when behaviour, configuration or guarantees change, and a new ADR in `docs/adr/` when a
  design decision changes. The README's configuration table must list every new setting with its default.
- A line under `Unreleased` in `CHANGELOG.md`.

Keep pull requests focused on one failure mode or one feature. The PR template asks which failure scenario the change
affects; "none" is a fine answer for refactors.

## Style

Formatting is enforced, not reviewed: `prettier` for JavaScript, `google-java-format` via Spotless for Java, `ruff
format` for Python. Run `make lint` before pushing.

## Editor and tooling safety

- `.vscode/` is git-ignored. Do not commit `tasks.json`; a task that runs on `folderOpen` executes before anyone
  reads the diff. Keep `"task.allowAutomaticTasks": "off"` in your VS Code user settings.
- Do not add `preinstall`, `install`, `postinstall` or `prepare` scripts to `package.json` that run `node`, `curl`
  or `wget`. Build-tool config files (`*.config.{js,mjs,cjs,ts}`) stay under 4 KB and contain no encoded blobs.
- The `a10-guard` workflow enforces both rules and is a required check on `main`. See SECURITY.md for the threat
  model and the indicator list.
