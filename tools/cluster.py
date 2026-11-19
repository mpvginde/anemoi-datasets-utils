#!/usr/bin/env python3
"""Cluster the single-tape runs of a tapegroups.py bisection into cartridge groups.

Phase 2 of tapegroups.py, separated so it can run from the logged bisection (costs.tsv)
without probing MARS again, in parallel, and resumable:

- runs are rebuilt from costs.tsv: bisection stops at every range reporting one tape, so
  every date-range row with tapes == 1 is a run (plus single days still on > 1 tape);
- for each run, the candidate clusters (most recently used first, up to --max-tries) are
  tested in parallel; the run joins the first candidate whose representative date plus the
  run still reports one tape, else starts a new cluster;
- the state per (request type, time) is saved after every run (state/*.json); a restart
  continues where it stopped;
- finally every cluster is checked once more and written as a `stage` request (req/) and a
  line in groups.tsv, as tapegroups.py does.

Concurrent MARS calls are capped by --max-mars over all threads.
"""
import argparse
import datetime as dt
import json
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor

import yaml

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import tapegroups as tg  # noqa: E402


def parse_date(s):
    return dt.datetime.strptime(s, "%Y%m%d").date()


def load_runs(costs):
    runs = {}
    with open(costs) as f:
        for line in f:
            name, first, last, times, tapes, files, off, fields = line.rstrip("\n").split("\t")
            if last.startswith("list:") or "/" in times:
                continue  # union tests of an earlier clustering, or multi-time rows
            tapes = int(tapes)
            if tapes == 1 or (first == last and tapes > 1):
                runs.setdefault((name, times), []).append((parse_date(first), parse_date(last), tapes))
    return {k: sorted(set(v)) for k, v in runs.items()}


class LimitedProber(tg.Prober):
    def __init__(self, outdir, max_mars):
        super().__init__(outdir)
        self.sem = threading.Semaphore(max_mars)

    def cost(self, *args, **kwargs):
        with self.sem:
            return super().cost(*args, **kwargs)


def cluster_one(prober, name, base, time, runs, max_tries, state_path, pool):
    state = {"done": 0, "clusters": [], "order": []}
    if os.path.exists(state_path):
        state = json.load(open(state_path))
        print(f"{name} {time}: resuming at run {state['done']}/{len(runs)}", flush=True)
    clusters = [{"rep": parse_date(c["rep"]), "dates": [parse_date(d) for d in c["dates"]],
                 "multi": c["multi"]} for c in state["clusters"]]
    order = state["order"]

    def save(done):
        tmp = state_path + ".tmp"
        json.dump({"done": done, "order": order,
                   "clusters": [{"rep": tg.fmt(c["rep"]), "dates": [tg.fmt(d) for d in c["dates"]],
                                 "multi": c["multi"]} for c in clusters]}, open(tmp, "w"))
        os.replace(tmp, state_path)

    for k in range(state["done"], len(runs)):
        d0, d1, tapes = runs[k]
        ds = tg.days(d0, d1)
        if tapes > 1:
            clusters.append({"rep": d0, "dates": ds, "multi": True})
        else:
            cands = order[:max_tries]
            futs = [pool.submit(prober.cost, name, base, [time], [clusters[i]["rep"]] + ds) for i in cands]
            hit = next((i for i, f in zip(cands, futs) if f.result()["number_of_tapes"] == 1), None)
            if hit is None:
                clusters.append({"rep": d0, "dates": ds, "multi": False})
                order.insert(0, len(clusters) - 1)
            else:
                clusters[hit]["dates"].extend(ds)
                order.remove(hit)
                order.insert(0, hit)
        save(k + 1)
        if (k + 1) % 50 == 0:
            print(f"{name} {time}: {k + 1}/{len(runs)} runs, {len(clusters)} clusters", flush=True)
    print(f"{name} {time}: {len(runs)} runs -> {len(clusters)} clusters", flush=True)
    return clusters


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("config", help="suite config with prestage.requests (as used for the bisection)")
    ap.add_argument("costs", help="costs.tsv of the bisection")
    ap.add_argument("outdir")
    ap.add_argument("--max-tries", type=int, default=12)
    ap.add_argument("--max-mars", type=int, default=12, help="concurrent MARS calls")
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config))
    verb = cfg["prestage"].get("verb", "stage")
    templates = {f"{r['stream']}-{r['levtype']}": r for r in cfg["prestage"]["requests"]}
    for sub in ("req", "state"):
        os.makedirs(os.path.join(args.outdir, sub), exist_ok=True)
    prober = LimitedProber(args.outdir, args.max_mars)
    runs = load_runs(args.costs)
    for (name, time), r in sorted(runs.items()):
        print(f"{name} {time}: {len(r)} runs", flush=True)

    inner = ThreadPoolExecutor(args.max_mars)
    with ThreadPoolExecutor(len(runs)) as outer:
        futs = {key: outer.submit(cluster_one, prober, key[0], templates[key[0]], key[1], r, args.max_tries,
                                  os.path.join(args.outdir, "state", f"{key[0]}_{key[1]}.json"), inner)
                for key, r in runs.items()}
        clusters = {key: f.result() for key, f in futs.items()}

    checks = {key: [inner.submit(prober.cost, key[0], templates[key[0]], [key[1]], c["dates"]) for c in cl]
              for key, cl in clusters.items()}
    with open(os.path.join(args.outdir, "groups.tsv"), "w") as out:
        out.write("request\ttype\ttime\tfirst\tlast\tndates\ttapes\ttape_files\toffline_GB\n")
        for (name, time), cl in sorted(clusters.items()):
            pairs = sorted(zip(cl, (f.result() for f in checks[(name, time)])), key=lambda x: min(x[0]["dates"]))
            for n, (c, cost) in enumerate(pairs, 1):
                tag = f"{name}_{time}_{n:03d}"
                with open(os.path.join(args.outdir, "req", tag + ".req"), "w") as f:
                    f.write(tg.mars_request(verb, templates[name], [time], c["dates"]))
                out.write(f"{tag}\t{name}\t{time}\t{tg.fmt(min(c['dates']))}\t{tg.fmt(max(c['dates']))}\t"
                          f"{len(set(c['dates']))}\t{cost['number_of_tapes']}\t"
                          f"{cost['number_of_tape_files']}\t{cost['off_line_size'] / 1e9:.2f}\n")
    inner.shutdown()


if __name__ == "__main__":
    main()
