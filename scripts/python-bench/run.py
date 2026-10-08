import asyncio
import os
import random
import uuid
import time
from typing import Dict

import aiohttp
import orjson
from rich.console import Console
from rich.table import Table

console = Console()

TARGET_URL = os.getenv("TARGET_URL", "http://localhost:3000/api/events")


def json_dumps(obj):
    return orjson.dumps(obj).decode()


def make_payload(counter: int) -> Dict:
    return {"amount": round(random.uniform(1, 100), 2), "note": f"txn-{counter}"}


async def send_one(session: aiohttp.ClientSession, payload: Dict, idem_key: str):
    try:
        async with session.post(TARGET_URL, json=payload, headers={"Idempotency-Key": idem_key}, timeout=10) as resp:
            data = await resp.json(content_type=None)
            return resp.status, data
    except Exception as exc:
        return 0, {"error": str(exc)}


async def run_profile(name: str, total: int, dup_ratio: float):
    stats = {"accepted": 0, "errors": 0, "processing": 0}
    idem_pool = []
    payloads = {}
    start = time.time()
    async with aiohttp.ClientSession(json_serialize=json_dumps) as session:
        tasks = []
        for i in range(total):
            if idem_pool and random.random() < dup_ratio:
                idem_key = random.choice(idem_pool)
            else:
                idem_key = str(uuid.uuid4())
                idem_pool.append(idem_key)
                payloads[idem_key] = make_payload(i)
            payload = payloads[idem_key]
            tasks.append(asyncio.create_task(send_one(session, payload, idem_key)))
        for coro in asyncio.as_completed(tasks):
            status, body = await coro
            if status in (200, 202) and body.get("status") == "processing":
                stats["processing"] += 1
            elif status in (200, 202):
                stats["accepted"] += 1
            else:
                stats["errors"] += 1
    duration = time.time() - start
    return stats, duration


async def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", choices=["normal", "dup30"], default="normal")
    parser.add_argument("--total", type=int, default=200)
    args = parser.parse_args()

    if args.profile == "normal":
        stats, dur = await run_profile("normal", args.total, dup_ratio=0.0)
    elif args.profile == "dup30":
        stats, dur = await run_profile("dup30", args.total, dup_ratio=0.3)
    else:
        raise SystemExit("unknown profile")

    table = Table(title=f"Profile {args.profile}")
    table.add_column("metric")
    table.add_column("value")
    table.add_row("accepted", str(stats["accepted"]))
    table.add_row("errors", str(stats["errors"]))
    table.add_row("processing", str(stats["processing"]))
    table.add_row("duration_sec", f"{dur:.2f}")
    console.print(table)


if __name__ == "__main__":
    asyncio.run(main())
