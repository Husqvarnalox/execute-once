# Contributing

## Prerequisites
Docker with Compose. For local development outside containers: Node.js 20, JDK 17, Maven 3.9, Python 3.11.

## Build and check
```bash
docker compose up -d --build            # full stack
mvn -B -f services/relay-java/pom.xml verify
mvn -B -f services/consumer-java/pom.xml verify
node --check services/gateway-node/index.js
docker compose config -q
```

After changing delivery or dedup logic, run the `dup30` profile and the invariant queries in `docs/experiments.md`.

## Pull requests
Keep changes focused, explain the failure mode a change addresses, and update `docs/` and `CHANGELOG.md` when
behavior changes.
