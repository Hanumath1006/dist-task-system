import argparse
import csv
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import grpc
import generated.task_pb2
import generated.task_pb2_grpc

TASKS = [
    ("sha256", "hello-world"),
    ("reverse", "distributed-systems"),
    ("fib", "30"),
    ("fib", "35"),
    ("sha256", "x" * 1000),
]

def one_call(addr: str):
    t0 = time.time()
    with grpc.insecure_channel(addr) as ch:
        api = task_pb2_grpc.ApiServiceStub(ch)

        task_type, payload = random.choice(TASKS)
        tid = api.SubmitTask(task_pb2.SubmitTaskRequest(
            task_type=task_type,
            payload=payload
        ), timeout=5).task_id

        res = api.GetResult(task_pb2.GetResultRequest(task_id=tid), timeout=5)

    dt_ms = (time.time() - t0) * 1000.0
    ok = res.status in ("DONE", "ERROR")
    return dt_ms, ok, res.status

def run(addr: str, total: int, concurrency: int, out_csv: str):
    latencies = []
    ok_count = 0
    t_start = time.time()

    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futs = [ex.submit(one_call, addr) for _ in range(total)]
        for f in as_completed(futs):
            dt_ms, ok, _ = f.result()
            latencies.append(dt_ms)
            if ok:
                ok_count += 1

    elapsed = time.time() - t_start
    throughput = total / elapsed if elapsed > 0 else 0.0
    latencies.sort()

    def pct(p):
        if not latencies:
            return 0.0
        idx = int(p * (len(latencies) - 1))
        return latencies[idx]

    summary = {
        "addr": addr,
        "total": total,
        "concurrency": concurrency,
        "elapsed_s": elapsed,
        "throughput_rps": throughput,
        "p50_ms": pct(0.50),
        "p95_ms": pct(0.95),
        "p99_ms": pct(0.99),
        "ok_count": ok_count,
    }

    print("SUMMARY:", summary)

    with open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["latency_ms"])
        for x in latencies:
            w.writerow([x])

    with open(out_csv.replace(".csv", ".summary.csv"), "w", newline="") as f:
        w = csv.writer(f)
        for k, v in summary.items():
            w.writerow([k, v])

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--addr", required=True, help="host:port (e.g., localhost:50050)")
    ap.add_argument("--total", type=int, default=50)
    ap.add_argument("--concurrency", type=int, default=10)
    ap.add_argument("--out", default="results.csv")
    args = ap.parse_args()
    run(args.addr, args.total, args.concurrency, args.out)

    