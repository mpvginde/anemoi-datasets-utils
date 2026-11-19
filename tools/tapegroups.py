#!/usr/bin/env python3
"""Group the v2 prestage requests by tape cartridge.

MARS does not name cartridges, but `list,output=cost` reports how many tapes a request
touches. Per prestage request type (from the suite config) and per base time:

1. bisect each year until every contiguous date range reports at most one tape
   (fully online ranges, 0 tapes, need no staging);
2. cluster those runs by cartridge: a run joins an existing cluster when one representative
   date of the cluster plus the run still reports one tape (the most recently used clusters
   are tried first);
3. write one `stage` request per cluster, with an explicit date list, and check it reports
   one tape.

Writes, in the output directory:
  costs.tsv    every cost call (type, first date, last date or list:<n>, times, tapes,
               tape files, offline bytes, fields)
  groups.tsv   the cartridge clusters
  req/<type>_<time>_<nn>.req   one `stage` request per cluster
"""
import argparse
import datetime as dt
import os
import re
import subprocess
import sys
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor

import yaml

FIELDS = ("number_of_tapes", "number_of_tape_files", "off_line_size", "number_of_fields")
lock = threading.Lock()


def fmt(d):
    return d.strftime("%Y%m%d")


def mars_request(verb, base, times, dates, extra=""):
    """dates: (first, last) range or an explicit list of dates."""
    lines = [f"{verb},"]
    for k, v in base.items():
        if k == "time":
            continue
        if isinstance(v, list):
            v = "/".join(str(x) for x in v)
        lines.append(f"    {k}={v},")
    lines.append(f"    time={'/'.join(times)},")
    if extra:
        lines.append(f"    {extra},")
    if isinstance(dates, tuple):
        lines.append(f"    date={fmt(dates[0])}/to/{fmt(dates[1])}")
    else:
        lines.append(f"    date={'/'.join(fmt(d) for d in sorted(set(dates)))}")
    return "\n".join(lines) + "\n"


class Prober:
    def __init__(self, outdir):
        self.out = open(os.path.join(outdir, "costs.tsv"), "a")

    def cost(self, name, base, times, dates):
        req = mars_request("list", base, times, dates, extra="output=cost")
        for attempt in range(3):
            with tempfile.NamedTemporaryFile("w", suffix=".req", delete=False) as f:
                f.write(req)
            try:
                p = subprocess.run(["mars", f.name], capture_output=True, text=True, timeout=1800)
            finally:
                os.unlink(f.name)
            vals = {k: int(m.group(1)) for k in FIELDS
                    for m in [re.search(rf"^{k}=(\d+);", p.stdout, re.M)] if m}
            if len(vals) == len(FIELDS):
                break
            print(f"retry {name} {times}: rc={p.returncode}", file=sys.stderr, flush=True)
        else:
            raise RuntimeError(f"cost failed for {name} {times} {dates}\n{p.stdout[-2000:]}{p.stderr[-2000:]}")
        if isinstance(dates, tuple):
            first, last = fmt(dates[0]), fmt(dates[1])
        else:
            first, last = fmt(min(dates)), f"list:{len(set(dates))}"
        with lock:
            self.out.write(f"{name}\t{first}\t{last}\t{'/'.join(times)}\t" +
                           "\t".join(str(vals[k]) for k in FIELDS) + "\n")
            self.out.flush()
        return vals


def bisect(prober, name, base, time, d0, d1, runs):
    """Contiguous ranges [d0, d1] on at most one tape, for one base time."""
    c = prober.cost(name, base, [time], (d0, d1))
    if c["number_of_tapes"] == 0:
        return
    if c["number_of_tapes"] == 1 or d0 == d1:  # d0 == d1 with >1 tape: one file split over tapes
        runs.append((d0, d1, c))
        return
    mid = d0 + (d1 - d0) // 2
    bisect(prober, name, base, time, d0, mid, runs)
    bisect(prober, name, base, time, mid + dt.timedelta(days=1), d1, runs)


def days(d0, d1):
    return [d0 + dt.timedelta(days=i) for i in range((d1 - d0).days + 1)]


def cluster(prober, name, base, time, runs, max_tries):
    """Assign runs to cartridge clusters; returns a list of date lists."""
    clusters = []  # each: {"rep": date, "dates": [...], "multi": bool}
    order = []     # cluster indices, most recently used first
    for d0, d1, c in sorted(runs, key=lambda r: r[0]):
        ds = days(d0, d1)
        if c["number_of_tapes"] > 1:  # unsplittable: its own group
            clusters.append({"rep": d0, "dates": ds, "multi": True})
            continue
        for i in order[:max_tries]:
            if prober.cost(name, base, [time], [clusters[i]["rep"]] + ds)["number_of_tapes"] == 1:
                clusters[i]["dates"].extend(ds)
                order.remove(i)
                order.insert(0, i)
                break
        else:
            clusters.append({"rep": d0, "dates": ds, "multi": False})
            order.insert(0, len(clusters) - 1)
    return clusters


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("config", help="suite config with prestage.requests")
    ap.add_argument("outdir")
    ap.add_argument("--start", default="2020-12-31")
    ap.add_argument("--end", default="2024-12-31")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--max-tries", type=int, default=12,
                    help="clusters tried (most recently used first) before starting a new one")
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config))
    verb = cfg["prestage"].get("verb", "stage")
    reqs = cfg["prestage"]["requests"]
    os.makedirs(os.path.join(args.outdir, "req"), exist_ok=True)
    prober = Prober(args.outdir)
    start, end = (dt.date.fromisoformat(x) for x in (args.start, args.end))

    spans, y = [], start
    while y <= end:
        y1 = min(dt.date(y.year, 12, 31), end)
        spans.append((y, y1))
        y = y1 + dt.timedelta(days=1)

    jobs = [(r, f"{r['stream']}-{r['levtype']}", str(t)) for r in reqs for t in r["time"]]

    def run_bisect(r, name, time, s0, s1):
        runs = []
        bisect(prober, name, r, time, s0, s1, runs)
        print(f"{name} {time} {s0}..{s1}: {len(runs)} runs", flush=True)
        return runs

    with ThreadPoolExecutor(args.workers) as ex:
        futs = {(name, time): [ex.submit(run_bisect, r, name, time, s0, s1) for s0, s1 in spans]
                for r, name, time in jobs}
        runs = {k: [x for f in v for x in f.result()] for k, v in futs.items()}

        def run_cluster(r, name, time):
            cl = cluster(prober, name, r, time, runs[(name, time)], args.max_tries)
            print(f"{name} {time}: {len(runs[(name, time)])} runs -> {len(cl)} clusters", flush=True)
            checked = []
            for c in cl:
                checked.append((c, prober.cost(name, r, [time], c["dates"])))
            return r, name, time, checked

        results = [f.result() for f in [ex.submit(run_cluster, r, name, time) for r, name, time in jobs]]

    with open(os.path.join(args.outdir, "groups.tsv"), "w") as out:
        out.write("request\ttype\ttime\tfirst\tlast\tndates\ttapes\ttape_files\toffline_GB\n")
        for r, name, time, checked in results:
            for n, (c, cost) in enumerate(sorted(checked, key=lambda x: min(x[0]["dates"])), 1):
                tag = f"{name}_{time}_{n:03d}"
                with open(os.path.join(args.outdir, "req", tag + ".req"), "w") as f:
                    f.write(mars_request(verb, r, [time], c["dates"]))
                out.write(f"{tag}\t{name}\t{time}\t{fmt(min(c['dates']))}\t{fmt(max(c['dates']))}\t"
                          f"{len(set(c['dates']))}\t{cost['number_of_tapes']}\t"
                          f"{cost['number_of_tape_files']}\t{cost['off_line_size'] / 1e9:.2f}\n")


if __name__ == "__main__":
    main()
