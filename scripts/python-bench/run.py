"""Load + verification tool for the execute-once pipeline.

Phase 1 (load): POST events to the gateway with Idempotency-Key headers.
Phase 2 (verify): poll Postgres until every returned event_id reached business_state, measure
end-to-end latency and check pipeline invariants.

Exit code: 0 when everything passed, 1 on any failed invariant/expectation or drain timeout.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime

import aiohttp
import asyncpg
from rich.console import Console
from rich.table import Table

console = Console()

DEFAULT_TARGET = "http://localhost:3000/api/events"
DEFAULT_DB = "postgresql://app:app@localhost:5432/app"
PROFILES = ("normal", "dup30", "burst")
OUTCOMES = ("accepted", "replayed", "conflict", "client_error", "server_error", "transport_error")


@dataclass
class Plan:
    """One request to send."""

    key: str
    payload: dict


@dataclass
class LoadResult:
    counts: dict[str, int] = field(default_factory=lambda: dict.fromkeys(OUTCOMES, 0))
    latencies_ms: list[float] = field(default_factory=list)
    event_ids: set[str] = field(default_factory=set)
    unique_keys_sent: int = 0
    confirmed_keys: set[str] = field(default_factory=set)
    duration_s: float = 0.0
    sample_errors: list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return sum(self.counts.values())

    @property
    def throughput(self) -> float:
        return self.total / self.duration_s if self.duration_s > 0 else 0.0


@dataclass
class Check:
    name: str
    status: str  # PASS | FAIL | INFO
    detail: str = ""


@dataclass
class VerifyResult:
    ran: bool = False
    error: str | None = None
    drained: bool = False
    drain_seconds: float = 0.0
    applied: int = 0
    expected: int = 0
    e2e_ms: list[float] = field(default_factory=list)
    checks: list[Check] = field(default_factory=list)


def percentiles(values: list[float]) -> dict[str, float]:
    """Nearest-rank percentiles; zeros for an empty sample."""
    if not values:
        return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}
    ordered = sorted(values)

    def rank(p: float) -> float:
        idx = max(0, min(len(ordered) - 1, int(-(-p * len(ordered) // 100)) - 1))
        return ordered[idx]

    return {"p50": rank(50), "p95": rank(95), "p99": rank(99), "max": ordered[-1]}


def build_plan(profile: str, total: int, run_id: str, rng: random.Random) -> list[Plan]:
    """Unique keys, except dup30 where ~30% of requests reuse an earlier key (same payload)."""
    dup_ratio = 0.3 if profile == "dup30" else 0.0
    plans: list[Plan] = []
    originals: list[Plan] = []
    for i in range(total):
        if originals and rng.random() < dup_ratio:
            plans.append(rng.choice(originals))
            continue
        plan = Plan(
            key=f"bench-{run_id}-{uuid.uuid4()}",
            payload={"amount": round(rng.uniform(1, 100), 2), "note": f"bench-{run_id}-{i}"},
        )
        originals.append(plan)
        plans.append(plan)
    return plans


async def send_one(session: aiohttp.ClientSession, url: str, plan: Plan, result: LoadResult) -> None:
    start = time.perf_counter()
    outcome = "transport_error"
    try:
        async with session.post(url, json=plan.payload, headers={"Idempotency-Key": plan.key}) as resp:
            raw = await resp.read()
            replayed = resp.headers.get("Idempotency-Replayed", "").lower() == "true"
            if resp.status == 202:
                outcome = "replayed" if replayed else "accepted"
                try:
                    event_id = json.loads(raw).get("event_id")
                except (ValueError, AttributeError):
                    event_id = None
                if event_id:
                    result.event_ids.add(event_id)
                    result.confirmed_keys.add(plan.key)
            elif resp.status == 409:
                outcome = "conflict"
            elif 400 <= resp.status < 500:
                outcome = "client_error"
            elif resp.status >= 500:
                outcome = "server_error"
            else:
                outcome = "client_error"
            if outcome in ("client_error", "server_error") and len(result.sample_errors) < 3:
                result.sample_errors.append(f"HTTP {resp.status}: {raw[:200]!r}")
    except (TimeoutError, aiohttp.ClientError, OSError) as exc:
        if len(result.sample_errors) < 3:
            result.sample_errors.append(f"{type(exc).__name__}: {exc}")
    result.latencies_ms.append((time.perf_counter() - start) * 1000)
    result.counts[outcome] += 1


async def run_load(url: str, plans: list[Plan], concurrency: int) -> LoadResult:
    result = LoadResult(unique_keys_sent=len({p.key for p in plans}))
    sem = asyncio.Semaphore(concurrency)
    connector = aiohttp.TCPConnector(limit=concurrency)
    timeout = aiohttp.ClientTimeout(total=15)

    async def bounded(plan: Plan) -> None:
        async with sem:
            await send_one(session, url, plan, result)

    start = time.perf_counter()
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        await asyncio.gather(*(bounded(p) for p in plans))
    result.duration_s = time.perf_counter() - start
    return result


async def wait_for_drain(
    conn: asyncpg.Connection, ids: list[uuid.UUID], timeout_s: float
) -> tuple[int, float]:
    start = time.monotonic()
    applied = 0
    while True:
        applied = await conn.fetchval(
            "SELECT count(*) FROM business_state WHERE event_id = ANY($1::uuid[])", ids
        )
        elapsed = time.monotonic() - start
        if applied >= len(ids) or elapsed >= timeout_s:
            return applied, elapsed
        await asyncio.sleep(1.0)


async def run_invariants(
    conn: asyncpg.Connection, ids: list[uuid.UUID], load: LoadResult, run_id: str, drained: bool
) -> list[Check]:
    like = f"bench-{run_id}-%"
    checks: list[Check] = []

    def add(name: str, ok: bool, detail: str) -> None:
        checks.append(Check(name, "PASS" if ok else "FAIL", detail))

    dup = await conn.fetchval(
        "SELECT count(*) FROM (SELECT event_id FROM business_state WHERE event_id = ANY($1::uuid[])"
        " GROUP BY event_id HAVING count(*) > 1) d",
        ids,
    )
    add("1. no duplicate business_state per event_id", dup == 0, f"duplicates={dup}")

    orphans = await conn.fetchval(
        "SELECT count(*) FROM inbox_events i WHERE i.event_id = ANY($1::uuid[])"
        " AND NOT EXISTS (SELECT 1 FROM business_state b WHERE b.event_id = i.event_id)",
        ids,
    )
    add("2. no inbox_events without business_state", orphans == 0, f"orphans={orphans}")

    published = await conn.fetchval(
        "SELECT count(*) FROM outbox_events o WHERE o.idem_key LIKE $1 AND o.status = 'PUBLISHED'"
        " AND NOT EXISTS (SELECT 1 FROM business_state b WHERE b.event_id = o.event_id)",
        like,
    )
    add("3. no PUBLISHED outbox without business_state (after drain)", published == 0, f"count={published}")

    distinct = await conn.fetchval(
        "SELECT count(DISTINCT event_id) FROM business_state WHERE event_id = ANY($1::uuid[])", ids
    )
    add("4. every returned event_id has business_state", distinct == len(ids), f"{distinct}/{len(ids)}")

    outbox_rows = await conn.fetchval("SELECT count(*) FROM outbox_events WHERE idem_key LIKE $1", like)
    confirmed, sent = len(load.confirmed_keys), load.unique_keys_sent
    # Requests lost to transport/server errors may or may not have committed, so allow that window only.
    add(
        "5. outbox rows for this run == unique keys",
        confirmed <= outbox_rows <= sent,
        f"outbox={outbox_rows} unique_keys={sent} confirmed={confirmed}",
    )

    failed = await conn.fetchval(
        "SELECT count(*) FROM outbox_events WHERE idem_key LIKE $1 AND status = 'FAILED'", like
    )
    add("6. no FAILED outbox rows for this run", failed == 0, f"failed={failed}")

    dups = await conn.fetchval(
        "SELECT count(*) FROM consumer_attempts WHERE event_id = ANY($1::uuid[]) AND status = 'DUPLICATE'",
        ids,
    )
    checks.append(Check("7. consumer DUPLICATE attempts (info)", "INFO", f"count={dups}"))

    stale = await conn.fetchval(
        "SELECT count(*) FROM outbox_events WHERE idem_key LIKE $1 AND status = 'PENDING'"
        " AND created_at < now() - interval '5 minutes'",
        like,
    )
    add("8. no PENDING outbox older than 5 min", stale == 0, f"stale={stale}")

    extra = max(0, outbox_rows - sent)
    add("9. duplicate effects == 0", dup + extra == 0, f"duplicate_effects={dup + extra}")
    add("drain completed", drained, "all events applied" if drained else "drain timeout")
    return checks


async def run_verify(db_url: str, load: LoadResult, run_id: str, drain_timeout: float) -> VerifyResult:
    out = VerifyResult(ran=True, expected=len(load.event_ids))
    try:
        conn = await asyncpg.connect(db_url, timeout=10)
    except (OSError, asyncpg.PostgresError, TimeoutError) as exc:
        out.error = f"cannot connect to database: {exc}"
        return out
    try:
        ids = [uuid.UUID(e) for e in load.event_ids]
        out.applied, out.drain_seconds = await wait_for_drain(conn, ids, drain_timeout)
        out.drained = out.applied >= len(ids)
        rows = await conn.fetch(
            "SELECT extract(epoch FROM (b.created_at - o.created_at)) * 1000 AS ms"
            " FROM outbox_events o JOIN business_state b ON b.event_id = o.event_id"
            " WHERE o.event_id = ANY($1::uuid[])",
            ids,
        )
        out.e2e_ms = [float(r["ms"]) for r in rows]
        out.checks = await run_invariants(conn, ids, load, run_id, out.drained)
    except (asyncpg.PostgresError, OSError) as exc:
        out.error = f"verification failed: {exc}"
    finally:
        await conn.close()
    return out


def load_expectations(profile: str, load: LoadResult) -> list[Check]:
    c = load.counts
    ok_total = c["accepted"] + c["replayed"]
    checks = [
        Check(
            "load: accepted + replayed == total",
            "PASS" if ok_total == load.total else "FAIL",
            f"{ok_total}/{load.total}",
        ),
        Check("load: zero conflicts", "PASS" if c["conflict"] == 0 else "FAIL", f"conflicts={c['conflict']}"),
    ]
    if profile in ("normal", "burst"):
        checks.append(
            Check(
                "load: no replays for unique keys",
                "PASS" if c["replayed"] == 0 else "FAIL",
                f"replayed={c['replayed']}",
            )
        )
    return checks


def render(args: argparse.Namespace, load: LoadResult, verify: VerifyResult, checks: list[Check]) -> None:
    t = Table(title=f"Load summary: profile={args.profile} total={args.total} concurrency={args.concurrency}")
    t.add_column("metric")
    t.add_column("value", justify="right")
    for name in OUTCOMES:
        t.add_row(name, str(load.counts[name]))
    t.add_row("unique event_ids", str(len(load.event_ids)))
    t.add_row("duration_s", f"{load.duration_s:.2f}")
    t.add_row("throughput req/s", f"{load.throughput:.1f}")
    console.print(t)
    for err in load.sample_errors:
        console.print(f"[yellow]sample error:[/yellow] {err}")

    lat = Table(title="Latency (ms)")
    for col in ("kind", "p50", "p95", "p99", "max"):
        lat.add_column(col, justify="right" if col != "kind" else "left")
    p = percentiles(load.latencies_ms)
    lat.add_row("http request", *(f"{p[k]:.1f}" for k in ("p50", "p95", "p99", "max")))
    if verify.e2e_ms:
        e = percentiles(verify.e2e_ms)
        lat.add_row("e2e (outbox -> business_state)", *(f"{e[k]:.1f}" for k in ("p50", "p95", "p99", "max")))
    console.print(lat)

    if verify.error:
        console.print(f"[red]verify error:[/red] {verify.error}")
    inv = Table(title="Invariants")
    inv.add_column("check")
    inv.add_column("result")
    inv.add_column("detail")
    styles = {"PASS": "green", "FAIL": "red", "INFO": "cyan"}
    for c in checks:
        inv.add_row(c.name, f"[{styles[c.status]}]{c.status}[/{styles[c.status]}]", c.detail)
    console.print(inv)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load + verification tool for execute-once")
    p.add_argument("--profile", choices=PROFILES, default="normal")
    p.add_argument("--total", type=int, default=200)
    p.add_argument("--concurrency", type=int, default=None, help="default 50 (200 for burst)")
    p.add_argument("--target-url", default=os.getenv("TARGET_URL", DEFAULT_TARGET))
    p.add_argument("--db-url", default=os.getenv("DB_URL", DEFAULT_DB))
    p.add_argument("--verify", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--drain-timeout", type=float, default=120.0, help="seconds to wait for processing")
    p.add_argument("--report", metavar="PATH", help="write a JSON report")
    p.add_argument("--seed", type=int, default=None, help="RNG seed for reproducible plans")
    args = p.parse_args(argv)
    if args.concurrency is None:
        args.concurrency = 200 if args.profile == "burst" else 50
    if args.total < 1 or args.concurrency < 1:
        p.error("--total and --concurrency must be >= 1")
    return args


async def run(args: argparse.Namespace) -> int:
    run_id = uuid.uuid4().hex[:8]
    started = datetime.now(UTC)
    plans = build_plan(args.profile, args.total, run_id, random.Random(args.seed))
    console.print(f"run {run_id}: sending {len(plans)} requests to {args.target_url}")
    load = await run_load(args.target_url, plans, args.concurrency)

    checks = load_expectations(args.profile, load)
    verify = VerifyResult()
    if args.verify:
        if load.event_ids:
            verify = await run_verify(args.db_url, load, run_id, args.drain_timeout)
            checks += verify.checks
            if verify.error:
                checks.append(Check("verification", "FAIL", verify.error))
        else:
            verify.ran = True
            checks.append(Check("verification", "FAIL", "no event_ids returned; nothing to verify"))

    render(args, load, verify, checks)
    failed = [c.name for c in checks if c.status == "FAIL"]

    if args.report:
        report = {
            "run_id": run_id,
            "timestamp": started.isoformat(),
            "profile": args.profile,
            "total": args.total,
            "concurrency": args.concurrency,
            "target_url": args.target_url,
            "load": {
                "counts": load.counts,
                "duration_s": load.duration_s,
                "throughput_rps": load.throughput,
                "latency_ms": percentiles(load.latencies_ms),
                "unique_event_ids": len(load.event_ids),
                "unique_keys_sent": load.unique_keys_sent,
                "sample_errors": load.sample_errors,
            },
            "verify": {
                "ran": verify.ran,
                "error": verify.error,
                "drained": verify.drained,
                "drain_seconds": verify.drain_seconds,
                "applied": verify.applied,
                "expected": verify.expected,
                "e2e_latency_ms": percentiles(verify.e2e_ms),
            },
            "checks": [asdict(c) for c in checks],
            "passed": not failed,
            "failed_checks": failed,
        }
        with open(args.report, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2)
        console.print(f"report written to {args.report}")

    console.print("[green]RESULT: PASS[/green]" if not failed else f"[red]RESULT: FAIL ({len(failed)})[/red]")
    return 0 if not failed else 1


def main() -> None:
    sys.exit(asyncio.run(run(parse_args())))


if __name__ == "__main__":
    main()
