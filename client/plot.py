import argparse, glob, csv
import matplotlib.pyplot as plt

def read_summary(path):
    d = {}
    with open(path, newline="") as f:
        for k, v in csv.reader(f):
            try:
                d[k] = float(v)
            except:
                d[k] = v
    return d

def load(pattern):
    rows = [read_summary(p) for p in sorted(glob.glob(pattern))]
    rows.sort(key=lambda x: x["concurrency"])
    return rows

def plot_latency(micro, mono, out):
    plt.figure()
    plt.plot([r["concurrency"] for r in micro], [r["p95_ms"] for r in micro], marker="o")
    plt.plot([r["concurrency"] for r in mono],  [r["p95_ms"] for r in mono],  marker="o")
    plt.xlabel("Concurrency")
    plt.ylabel("p95 latency (ms)")
    plt.legend(["Microservices", "Monolith"])
    plt.title("Latency vs Workload")
    plt.savefig(out + "_latency.png", dpi=200)

def plot_throughput(micro, mono, out):
    plt.figure()
    plt.plot([r["concurrency"] for r in micro], [r["throughput_rps"] for r in micro], marker="o")
    plt.plot([r["concurrency"] for r in mono],  [r["throughput_rps"] for r in mono],  marker="o")
    plt.xlabel("Concurrency")
    plt.ylabel("Throughput (req/s)")
    plt.legend(["Microservices", "Monolith"])
    plt.title("Throughput vs Workload")
    plt.savefig(out + "_throughput.png", dpi=200)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="results")
    args = ap.parse_args()

    micro = load("micro_*.summary.csv")
    mono  = load("mono_*.summary.csv")

    plot_latency(micro, mono, args.out)
    plot_throughput(micro, mono, args.out)