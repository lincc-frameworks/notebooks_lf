#!/usr/bin/env python
"""Write plot_benchmark.ipynb (kept as a generator so the notebook source is diff-able)."""

import argparse
import json
from pathlib import Path

ap = argparse.ArgumentParser(description="write the plotting notebook (optionally for another results config)")
ap.add_argument("--config", default="default", help="results/<config> with the astropy/smatch/... series")
ap.add_argument("--radec-config", default="radec", help="results/<config> with the RA/Dec-only lsdb series (skipped if absent)")
ap.add_argument("--slurm-config", default="default_slurm", help="results/<config> with the dedicated-node lsdb series (skipped if absent)")
ap.add_argument("--png", default="crossmatching-performance-2026.png")
ap.add_argument("--notebook", default="plot_benchmark.ipynb")
ap.add_argument("--title", default="Crossmatch Performance by Tool")
ap.add_argument("--legend-ncol", type=int, default=1, help="legend columns")
ap.add_argument("--legend-pos", choices=["inside", "below"], default="inside",
                help="inside: upper left of the axes (2025 layout); below: under the x axis, for ladders whose curves fill the upper left")
ap.add_argument("--x", choices=["matched", "left", "read"], default="matched",
                help="x axis: 'matched' = crossmatched rows (2025 layout); 'left' = rows of the left catalog in the selection; "
                     "'read' = rows of every left partition the selection touches (what the tools decode)")
ap.add_argument("--smatch-config", default="none",
                help="results/<config> whose smatch records are plotted as THE smatch series (e.g. cones_smatch_shuffled: second argument in "
                     "random order); the main config's smatch series is then kept as 'smatch_plain' (tables; plotted with --smatch-plain dotted)")
ap.add_argument("--smatch-plain", choices=["none", "dotted"], default="none", help="with --smatch-config: also draw the plain-call smatch series as a thin dotted line")
ap.add_argument("--aspect", choices=["auto", "equal"], default="auto",
                help="equal: one decade is the same length on both log axes (linear scaling = 45 degrees), wider figure, "
                     "plus a slope-1 guide line through the last LSDB 16-worker point")
ap.add_argument("--stilts-series", choices=["io", "xmatch"], default="io",
                help="io: tcat Parquet->FITS conversions + tmatch2 (STILTS as a black box on the HATS files); xmatch: tmatch2 alone, FITS input prepared")
OPTS = ap.parse_args()

CELLS = []


def md(src):
    CELLS.append({"cell_type": "markdown", "id": f"cell-{len(CELLS)}", "metadata": {}, "source": src})


def code(src):
    CELLS.append({"cell_type": "code", "id": f"cell-{len(CELLS)}", "metadata": {}, "execution_count": None, "outputs": [], "source": src})


md(
    """# Crossmatch performance by tool (2026 rerun on SLAC SDF)

Reads the per-run JSON files written by `run_benchmark.py` under `results/<config>/<step>/{lsdb_w<w>,astropy,smatch}_run<i>.json`
and reproduces the layout of the Feb 2025 figure (`../../lsdb_crossmatch_benchmarking/plot_benchmark.ipynb`).

Plotted quantity per tool, per ladder step:
* **LSDB (w worker threads)** = wall time of `crossmatch(...).map_partitions(len).compute()` (reads + matches).
* **Astropy** = pyarrow read of the same ZTF / Gaia / Gaia-margin files + Gaia/margin merge + cone filter + `SkyCoord`
  construction + `match_coordinates_sky` + distance cut + parquet write (native mode, 2026-09-17; the 2025 run excluded
  `SkyCoord` construction).
* **smatch** = the same pyarrow read + Gaia/margin merge + cone filter + `smatch.match(ztf, r, gaia, maxmatch=1)` + parquet
  write (native mode). With `--smatch-config`, the plotted series passes the Gaia side in random order (smatch's HEALPix
  tree is an unbalanced BST that HATS order degenerates into a chain, see NOTES.md); the plain call stays as `smatch_plain`.

Point = median over repeats; error bars = 10th/90th percentile; x = number of crossmatched rows, or of left-catalog
(ZTF) rows in the selection when the notebook was generated with `--x left`.
"""
)

code(
    """import json
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

RESULTS = Path("results")
# (config, series, step) triples left out of the curves. Both were run while the 16 x 8 GB Dask workers
# hit their 95% memory budget and were restarted (40 restarts in total, run_all log 18:00-20:45 on 2026-09-15):
# tasks were recomputed inside the timed region and the two repeats at pixels:1791 disagree by 2x
# (852 s vs 1668 s). The JSONs stay under results/default/ and are listed in the "excluded" table below.
EXCLUDE = {("default", "lsdb_16", "pixels:1024"), ("default", "lsdb_16", "pixels:1791")}
CONFIG_ALL = "__CONFIG__"   # ZTF default columns + 27 Gaia columns
CONFIG_RADEC = "__RADEC__"   # only ra, dec on both sides
CONFIG_SLURM = "__SLURM__"  # default columns, the largest steps re-run on a dedicated SLURM node (optional)
OUT_PNG = "__PNG__"
X_AXIS = "__XAXIS__"  # "matched": crossmatched rows; "left": rows of the left catalog (ZTF) in the selection; "read": rows of the partitions touched
XCOL = {"matched": "rows", "left": "left", "read": "read"}[X_AXIS]
STILTS_SERIES = "__STILTS__"  # "io": conversions + tmatch2; "xmatch": tmatch2 only
CONFIG_SMATCH = "__SMATCH__"  # results/<config> with smatch's second argument in random order ("none": plot the main config's smatch)
SMATCH_PLAIN = "__SMATCHPLAIN__"  # "dotted": also draw the main config's plain-call smatch series
ASPECT = "__ASPECT__"  # "equal": one decade the same length on both log axes
""".replace("__ASPECT__", OPTS.aspect).replace("__SMATCH__", OPTS.smatch_config).replace("__SMATCHPLAIN__", OPTS.smatch_plain).replace("__XAXIS__", OPTS.x).replace("__STILTS__", OPTS.stilts_series).replace("__CONFIG__", OPTS.config).replace("__RADEC__", OPTS.radec_config).replace("__SLURM__", OPTS.slurm_config).replace("__PNG__", OPTS.png)
)

code(
    """def load_runs(config):
    \"\"\"All per-(step, component, repeat) records of one config, plus its settings.\"\"\"
    recs = [json.loads(f.read_text()) for f in sorted((RESULTS / config).glob("*/*_run*.json"))]
    settings = json.loads((RESULTS / config / "settings.json").read_text())
    return recs, settings


def series_from_recs(recs, config):
    \"\"\"-> {series_key: {step: [(time, result_length, left_rows, left_rows_read), ...]}}; (config, series, step) in EXCLUDE are dropped\"\"\"
    out = defaultdict(lambda: defaultdict(list))
    for r in recs:
        step = r["step"]
        if r["tool"] == "lsdb":
            xm = r.get("lsdb_crossmatch")
            if xm and xm.get("result_length") is not None and (config, f"lsdb_{r['workers']}", step) not in EXCLUDE:
                out[f"lsdb_{r['workers']}"][step].append((xm["time"], xm["result_length"], r.get("left_rows"), r.get("left_rows_read")))
        elif r.get("ok"):
            tool = r["tool"]; ms = r["measurements"]
            # everything a user must do besides the match call itself: pyarrow reads (or STILTS conversions), joining Gaia with
            # its margin, the cone filter, SkyCoord construction, writing the result
            io = sum(v["time"] for k, v in ms.items() if isinstance(v, dict)
                     and (k.endswith("_pyarrow") or k.endswith("_io") or k.endswith("_write") or k in ("cone_filter", "merge_right", "skycoords")))
            xm = ms[f"{tool}_xmatch"]
            left = r.get("left_rows", ms.get("left_rows")); read = r.get("left_rows_read")
            out[f"{tool}+io"][step].append((io + xm["time"], xm["result_length"], left, read))
            out[f"{tool}_xmatch_only"][step].append((xm["time"], xm["result_length"], left, read))
            out["pyarrow_io"][step].append((io, xm["result_length"], left, read))
    return out


def failures(recs):
    \"\"\"Steps where astropy/smatch did not finish (out of memory or skipped).\"\"\"
    rows = [{"step": r["step"], "tool": r["tool"], "run": r["run"], "oom": r.get("oom"), "skipped": r.get("skipped"),
             "error": str(r.get("error", ""))[-120:]} for r in recs if r["tool"] != "lsdb" and not r.get("ok")]
    return pd.DataFrame(rows)


def aggregate(series):
    \"\"\"-> {series_key: DataFrame[step, n, rows, left, median, q10, q90]} sorted by the x column\"\"\"
    agg = {}
    for key, per_step in series.items():
        rows = []
        for step, vals in per_step.items():
            t = np.array([v[0] for v in vals]); n = np.array([v[1] for v in vals])
            left = [v[2] for v in vals if v[2] is not None]; read = [v[3] for v in vals if v[3] is not None]
            rows.append({"step": step, "n": len(t), "rows": int(np.median(n)), "left": int(np.median(left)) if left else None,
                         "read": int(np.median(read)) if read else None,
                         "median": np.median(t), "q10": np.quantile(t, 0.1), "q90": np.quantile(t, 0.9)})
        df = pd.DataFrame(rows)
        if XCOL in ("left", "read") and df[XCOL].isna().any():
            raise ValueError(f"{key}: {XCOL} rows missing for steps {df.loc[df[XCOL].isna(), 'step'].tolist()} (old results?)")
        agg[key] = df.sort_values(XCOL).reset_index(drop=True)
    return agg


recs_all, settings_all = load_runs(CONFIG_ALL)
agg = aggregate(series_from_recs(recs_all, CONFIG_ALL))
settings_slurm = None
for cfg, suffix in ((CONFIG_RADEC, "radec"), (CONFIG_SLURM, "slurm")):
    try:
        recs_cfg, settings_cfg = load_runs(cfg)
    except FileNotFoundError:
        print(f"no {cfg} config")
        continue
    if suffix == "slurm":
        settings_slurm = settings_cfg
    for k, df in aggregate(series_from_recs(recs_cfg, cfg)).items():
        if k.startswith("lsdb_"):
            agg[f"{k}_{suffix}"] = df
settings_smatch = None
if CONFIG_SMATCH != "none":
    # the shuffled-input smatch run replaces the plotted smatch series; the plain call stays as smatch_plain
    recs_sm, settings_smatch = load_runs(CONFIG_SMATCH)
    agg_sm = aggregate(series_from_recs(recs_sm, CONFIG_SMATCH))
    for k in ("smatch+io", "smatch_xmatch_only"):
        if k in agg:
            agg[k.replace("smatch", "smatch_plain", 1)] = agg.pop(k)
        if k in agg_sm:
            agg[k] = agg_sm[k]
print(sorted(agg))
failures(recs_all)
"""
)

code(
    """# Runs excluded from the curves (see EXCLUDE above): kept on disk, shown here
excluded = pd.DataFrame(
    [{"config": CONFIG_ALL, "step": r["step"], "workers": r["workers"], "run": r["run"],
      "time_s": round(r["lsdb_crossmatch"]["time"], 1), "rows": r["lsdb_crossmatch"]["result_length"]}
     for r in recs_all if r["tool"] == "lsdb" and (CONFIG_ALL, f"lsdb_{r['workers']}", r["step"]) in EXCLUDE]
)
excluded
"""
)

code(
    """# result_length must agree across tools for every step (a mismatch is a bug, not noise)
check = {}
for key, df in agg.items():
    if key.endswith("_xmatch_only") or key == "pyarrow_io":
        continue
    check[key] = df.set_index("step")["rows"]
pd.DataFrame(check)
"""
)

code(
    """# Left-catalog rows per step (identical for every tool by construction; the x axis when X_AXIS == "left")
pd.DataFrame({k: df.set_index("step")["left"] for k, df in agg.items() if not k.endswith("_xmatch_only") and k != "pyarrow_io"})
"""
)

code(
    """# Median-time table (seconds), indexed by step: the x value can repeat between steps (rows read) and a series may end early
tbl = pd.DataFrame({k: v.set_index("step")["median"].round(2) for k, v in agg.items() if not k.endswith("_xmatch_only")})
tbl.insert(0, XCOL, pd.concat([v.set_index("step")[XCOL] for v in agg.values()]).groupby(level=0).first())
tbl.sort_values(XCOL)
"""
)

code(
    """plt.style.use("default")
plt.rcParams.update({"font.size": 16, "axes.labelsize": 20, "axes.titlesize": 20, "legend.fontsize": 14})
LEGEND_POS = "__LEGEND_POS__"
fig, ax = plt.subplots(1, 1, figsize=(16, 8.3) if ASPECT == "equal" else (11, 7.5 if LEGEND_POS == "inside" else 8.8))

s = settings_all
scipy_threads = s.get("tool_threads") or s["n_cpus"]          # before --tool-threads existed scipy used every allowed core
stilts_threads = s.get("tool_threads") or min(6, s["n_cpus"])  # STILTS runner=parallel defaults to min(6, cores)
plotting_args = {
    "lsdb_1": dict(marker="o", label="LSDB (1 worker thread)", c="#012970", ls="-", lw=2),
    "lsdb_4": dict(marker="o", label="LSDB (4 worker threads)", c="#012970", ls="--", lw=2),
    "lsdb_16": dict(marker="o", label="LSDB (16 worker threads)", c="#012970", ls="-.", lw=2),
    "lsdb_16_radec": dict(marker="o", label="LSDB (16 worker threads, only RA and Dec columns)", c="#A74482", ls="-.", lw=2),
    "lsdb_16_slurm": dict(marker="s", label="LSDB (16 worker threads, dedicated SLURM node)", c="#012970", ls=":", lw=2, ms=9),
    "astropy+io": dict(marker="o", label="Astropy", c="orange", ls="-", lw=2),
    "smatch+io": dict(marker="o", label="smatch", c="#62A87C", ls="-", lw=2),  # the shuffled-input call when --smatch-config is set (footer note says so)
    "smatch_plain+io": dict(marker="o", label="smatch (plain call, HATS-sorted input)", c="#62A87C", ls=":", lw=1.5, ms=4),
    "stilts+io": dict(marker="s", label=f"STILTS tmatch2 (TOPCAT, {stilts_threads} worker threads)", c="#C0392B", ls="-", lw=2),
    "stilts_xmatch_only": dict(marker="s", label=f"STILTS tmatch2 only ({stilts_threads} worker threads)", c="#C0392B", ls="-", lw=2),
    "scipy+io": dict(marker="^", label=f"scipy cKDTree ({scipy_threads} worker threads)", c="#7F7F7F", ls=":", lw=2),
}
for key, kw in plotting_args.items():
    if key not in agg or key == ("stilts_xmatch_only" if STILTS_SERIES == "io" else "stilts+io"):
        continue
    if key == "smatch_plain+io" and SMATCH_PLAIN != "dotted":
        continue
    d = agg[key]
    x, y = d[XCOL].to_numpy(), d["median"].to_numpy()
    ax.errorbar(x, y, [y - d["q10"].to_numpy(), d["q90"].to_numpy() - y], capsize=5, **kw)

ax.set_xscale("log"); ax.set_yscale("log")
if ASPECT == "equal":
    # one decade is the same length on both axes, so time proportional to rows is a 45-degree line; the dashed guide has slope 1
    ax.set_aspect("equal", adjustable="box")
    ylo, yhi = ax.get_ylim()  # autoscaled from the series; the guide stays inside it
    ref = agg["lsdb_16"].sort_values(XCOL); x1, y1 = float(ref[XCOL].iloc[-1]), float(ref["median"].iloc[-1])
    ax.plot([x1 * ylo / y1, x1], [ylo, y1], color="0.45", lw=1.2, ls="--", zorder=0, label="slope 1 (time proportional to rows)")
    ax.set_ylim(ylo, yhi)
ax.tick_params(axis="both", which="major", labelsize=18, length=12, direction="in")
ax.tick_params(axis="both", which="minor", labelsize=12, length=6, direction="in")
ax.set_xlabel({"matched": "Number of Crossmatched Rows", "left": "Number of Rows in the Left Catalog",
               "read": "Number of Left-Catalog Rows Read (partitions touched)"}[X_AXIS])
ax.set_ylabel("Time taken (s)")
ax.set_title("__TITLE__")
if LEGEND_POS == "inside":
    ax.legend(loc="upper left", ncol=__LEGEND_NCOL__, fontsize=13 if __LEGEND_NCOL__ > 1 else 14, columnspacing=1.2)
else:
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.13), ncol=__LEGEND_NCOL__, fontsize=14, frameon=False, columnspacing=2.0)

v = s["versions"]
threads = {str(r["workers"]): r["threads_per_worker"] for r in recs_all if r["tool"] == "lsdb"}
steps_all = {r["step"] for r in recs_all}
seed = s["seed_partition"]
sel = (f", cones centred on RA {seed['ra']:.2f} Dec {seed['dec']:+.2f}" if all(st.startswith("cone") for st in steps_all) else "")
mode = "results written to parquet" if s.get("output_mode") == "write" else "results counted in memory"
band = f", ZTF partitions {s['rows_band'][0]/1e6:.1f}-{s['rows_band'][1]/1e6:.1f}M rows" if s.get("rows_band") else ""
stilts_note = " | STILTS: tmatch2 only, Parquet-to-FITS conversion excluded" if STILTS_SERIES == "xmatch" else ""
stilts_note += " | smatch: Gaia side in random order (its hpix tree is an unbalanced BST)" if settings_smatch is not None else ""
caption = (f"SLAC SDF {s['hostname']}, {s['n_cpus']} cores, {s['mem_cap_gb']:.0f} GB cap, warm page cache, {mode}{band}{sel}{stilts_note} | "
           f"lsdb {v['lsdb']}, dask {v['dask']}, astropy {v['astropy']}, smatch {v['smatch']} | "
           f"ZTF DR23 objects x Gaia DR3, {s['radius_arcsec']:g} arcsec nearest neighbour | "
           f"{threads.get('1', 1)} thread per Dask worker | {s['timestamp_utc'][:10]}")
if settings_slurm is not None:
    caption += (f" | squares: {settings_slurm['hostname']}, {settings_slurm['n_cpus']} cores, "
                f"{settings_slurm['mem_cap_gb']:.0f} GB cap")
import textwrap
fig.text(0.5, 0.005, textwrap.fill(caption, 160), ha="center", va="bottom", fontsize=9, color="0.35")
fig.tight_layout(rect=(0, 0.04, 1, 1))
fig.savefig(OUT_PNG, dpi=300)
print("wrote", OUT_PNG)
"""
)

nb = {
    "cells": CELLS,
    "metadata": {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python"},
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}
for c in CELLS:
    if c["cell_type"] == "code":
        c["source"] = c["source"].replace("__TITLE__", OPTS.title).replace("__LEGEND_NCOL__", str(OPTS.legend_ncol)).replace("__LEGEND_POS__", OPTS.legend_pos)
Path(__file__).with_name(OPTS.notebook).write_text(json.dumps(nb, indent=1))
print("wrote", OPTS.notebook)
