# Tape-map tools (MARS prestage by cartridge)

Build the `prestage.tape_groups` request set that `create_suite.py` turns into one
`prestage/g_<group>/stage` task per tape cartridge (just-in-time staging, see
`_define_prestage_tape_groups`). Made for the IFS fc trajectory suite v2
(`ecflow/config-ifs-trajectory-v2.yml`, campaign `c2026-11-ifs-finetune-datasets`).

MARS does not name cartridges; `list,output=cost` only reports how many tapes a request
touches. The tools probe with that, so they only see what is offline at probe time (the MARS
disk cache turns over within hours).

1. `tapegroups.py <config> <outdir>`: per request type of `prestage.requests` and per base
   time, bisect the period into contiguous date ranges on at most one tape; every call is
   logged in `<outdir>/costs.tsv`.
2. `cluster.py <config> <outdir>/costs.tsv <cluster-outdir>`: rebuild the single-tape runs
   from `costs.tsv` (no new bisection), cluster them by cartridge with parallel MARS calls,
   resumable (`state/`), write `req/*.req` (one `stage` request per cluster) and
   `groups.tsv` (dates, tapes, offline size per group, checked once more at the end).
3. Point `prestage.tape_groups` at `<cluster-outdir>/req` and regenerate the suite.

Run both through `tapegroups.sbatch` (QoS `nf`); the full 2020-12-31..2024-12-31 map took
about 1 day of bisection and 6 h of clustering.
