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
