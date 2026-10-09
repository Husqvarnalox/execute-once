# Security

## Scope

This is a reference implementation for idempotent event processing. It is meant to be read, run locally and used as a
template, not deployed as-is. In particular:

- The gateway has no authentication or authorisation. Put it behind your own edge if you deploy it.
- `docker-compose.yml` and `.env.example` contain development credentials for the local network only.
- There is no TLS between services.

## Reporting

If you find a vulnerability in the code itself (for example a way to bypass the idempotency check, forge a replay, or
execute SQL through a request), please use GitHub's private vulnerability reporting on this repository rather than a
public issue. Reports are acknowledged within a week.

## Supply-chain hardening

This repository is public and therefore a target for the "A10 / Contagious Interview" pattern: a stolen credential is
used to force-push a forged tip commit that keeps the real parent and tree but adds a loader. The loader runs at
`npm install`, at build time, or when VS Code opens the folder. Defences in place:

- Branch ruleset on `main`: force pushes and deletion blocked, no bypass actors, signed commits, changes via pull request only,
  the `a10-guard` and CI status checks required. Release tags `v*` cannot be moved or deleted.
- `.github/workflows/a10-guard.yml` scans every push and pull request with git, grep, awk and xxd only, and fails on
  the indicators listed below. It never installs or runs anything from the repository.
- GitHub Actions: `GITHUB_TOKEN` is read-only by default, every action in every workflow is pinned to a commit SHA and the repository requires SHA pinning,
  workflows from external contributors need approval, `pull_request_target` is forbidden.
- `.github/CODEOWNERS` covers `.vscode/`, `.github/`, `public/fonts/`, `*.config.*`, `package.json` and lockfiles.

### Editor safety

Never let an editor auto-run tasks from a checkout. In VS Code keep `"task.allowAutomaticTasks": "off"` in your
user settings; `.vscode/` is git-ignored here, so a `tasks.json` that appears in a checkout did not come from this
project. Open unknown repositories in Restricted Mode.

### Indicators of compromise

Treat any of these in a commit, pull request or checkout as an incident. Do not run the code. Report as described
above, and if your own credential may be involved, rotate it first.

| Where | Indicator |
| --- | --- |
| Commit | Author timezone `+0300`, committer timezone `-0700`, identical epoch, unsigned |
| Commit message | CP437 mojibake such as `Γ ╬ ╠ ╢ ╤ ╦ ╨ ╚ ║` |
| Config files | `postcss.config.mjs`, `vite.config.mjs`, `next/tailwind/webpack/eslint` configs of 13–35 KB with an obfuscated tail |
| Fonts | `public/fonts/fa-solid-{400,500,600,700,900}.{woff,woff2,eot,ttf,otf}` that are JavaScript, not fonts |
| Editor | `.vscode/tasks.json` with `"runOn": "folderOpen"`, `"hide": true`, `"reveal": "never"`, `node ./public/fonts/...` |
| Dead drop | Ethereum wallet `0xa322E5f3D311D3080e6f0121063e9aDC2490Ef1a` read via `eth_call` on publicnode, drpc.org, blockscout, 1rpc.io; env `INDEXER_URL` |
| C2 | `193.247.144.38:443`, plain HTTP, paths `/0x/cls` and `/0x/ls` |
