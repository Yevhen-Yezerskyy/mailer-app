# FILE: engine/common/cache/bench_cache.py  (новое — 2026-01-05)
# PURPOSE: Бенч кеш-демона по локальному UNIX-socket: SET/GET/GET-miss пачками (по 5000 ключей),
#          замер времени и пропускной способности. Тестирует small и big payload.

from __future__ import annotations

import argparse
import os
import random
import string
import time
from typing import List, Tuple

from engine.common.cache.client import CLIENT


def _rand_key(prefix: str, i: int) -> str:
    return f"{prefix}:{i}"


def _rand_bytes(n: int) -> bytes:
    return os.urandom(n)


def _chunked(items: List[str], n: int) -> List[List[str]]:
    return [items[i : i + n] for i in range(0, len(items), n)]


def _bench_set(keys: List[str], payload: bytes, ttl_sec: int) -> Tuple[float, int]:
    t0 = time.perf_counter()
    ok = 0
    for k in keys:
        if CLIENT.set(k, payload, ttl_sec=ttl_sec):
            ok += 1
    dt = time.perf_counter() - t0
    return dt, ok


def _bench_get(keys: List[str], ttl_sec: int) -> Tuple[float, int]:
    t0 = time.perf_counter()
    hit = 0
    for k in keys:
        v = CLIENT.get(k, ttl_sec=ttl_sec)
        if v is not None:
            hit += 1
    dt = time.perf_counter() - t0
    return dt, hit


def _bench_get_miss(keys: List[str], ttl_sec: int) -> Tuple[float, int]:
    t0 = time.perf_counter()
    miss = 0
    for k in keys:
        v = CLIENT.get(k, ttl_sec=ttl_sec)
        if v is None:
            miss += 1
    dt = time.perf_counter() - t0
    return dt, miss


def _print_line(title: str, n: int, dt: float, cnt: int) -> None:
    rps = (n / dt) if dt > 0 else 0.0
    print(f"{title:<18} n={n:<6} dt={dt:.4f}s  rps={rps:,.0f}/s  cnt={cnt}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=5000, help="count of keys")
    ap.add_argument("--ttl", type=int, default=3600, help="ttl seconds")
    ap.add_argument("--rounds", type=int, default=3, help="repeat rounds")
    ap.add_argument("--small-bytes", type=int, default=64, help="payload size for small test")
    ap.add_argument("--big-bytes", type=int, default=16384, help="payload size for big test (e.g. 16KB)")
    ap.add_argument("--prefix", type=str, default="bench", help="key prefix")
    ap.add_argument("--shuffle", action="store_true", help="shuffle keys each round")
    args = ap.parse_args()

    n = int(args.n)
    ttl = int(args.ttl)
    rounds = int(args.rounds)

    # quick daemon sanity
    st = CLIENT.stats()
    print("STATS:", st)

    keys = [_rand_key(args.prefix, i) for i in range(n)]
    keys_miss = [_rand_key(args.prefix + "_miss", i) for i in range(n)]

    small_payload = _rand_bytes(int(args.small_bytes))
    big_payload = _rand_bytes(int(args.big_bytes))

    def run_suite(label: str, payload: bytes) -> None:
        print(f"\n=== {label} payload_bytes={len(payload)} ===")
        for r in range(1, rounds + 1):
            if args.shuffle:
                random.shuffle(keys)
                random.shuffle(keys_miss)

            dt_set, ok = _bench_set(keys, payload, ttl_sec=ttl)
            _print_line(f"SET r{r}", n, dt_set, ok)

            dt_get, hit = _bench_get(keys, ttl_sec=ttl)
            _print_line(f"GET r{r}", n, dt_get, hit)

            dt_miss, miss = _bench_get_miss(keys_miss, ttl_sec=ttl)
            _print_line(f"GET-miss r{r}", n, dt_miss, miss)

    run_suite("SMALL", small_payload)
    run_suite("BIG", big_payload)


if __name__ == "__main__":
    main()
