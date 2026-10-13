"""Six plane partitions, one worker. A: lsdb write_catalog with the scheduler's managed/unmanaged memory split and the task stream,
so the 5 GiB humps can be attributed. B: the write fused into the crossmatch task via map_partitions (one partition in, written, released)."""
import os, sys, time, threading, warnings, logging, json
warnings.filterwarnings("ignore"); logging.getLogger("distributed").setLevel(logging.ERROR)
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # the benchmark directory
import psutil, numpy as np, pandas as pd
import run_benchmark as rb
from lsdb import PixelSearch
from hats.pixel_math import HealpixPixel
from dask.distributed import Client, LocalCluster, get_task_stream
OUT = os.environ.get("XMATCH_OUT_DIR", f"/sdf/data/rubin/user/{os.environ.get('USER', 'user')}/xmatch-bench-out") + "/plane-test"
DEF = {"ztf": rb.ZTF_DEFAULT_COLS, "gaia": rb.GAIA_DEFAULT_COLS}
SIX = [HealpixPixel(4, p) for p in (921, 909, 901, 879, 912, 2007)]

def sched_mem(dask_scheduler):
    w = next(iter(dask_scheduler.workers.values())); m = w.memory
    return (m.process, m.managed, m.unmanaged_old, m.unmanaged_recent, len(w.has_what))

def run_case(name, fused, mem_gb=60):
    cluster = LocalCluster(n_workers=1, threads_per_worker=1, memory_limit=f"{mem_gb}GB", processes=True,
                           local_directory=f"/lscratch/{os.environ['USER']}/dask-plane-test", dashboard_address=":0")
    client = Client(cluster)
    proc = psutil.Process(next(iter(client.run(os.getpid).values())))
    samples, stop = [], threading.Event()
    def poll():
        while not stop.is_set():
            try:
                rss = proc.memory_info().rss
                m = client.run_on_scheduler(sched_mem)
                samples.append((time.monotonic(), rss, *m))
            except Exception:
                pass
            time.sleep(0.5)
    th = threading.Thread(target=poll, daemon=True); th.start()
    ztf, gaia = rb.open_catalogs(DEF)
    sel = ztf.search(PixelSearch(SIX)); gsel = gaia.search(PixelSearch(sel.get_healpix_pixels()))
    xm = sel.crossmatch(gsel, radius_arcsec=10, n_neighbors=1, suffixes=("_ztf", "_gaia"), suffix_method="all_columns")
    out = f"{OUT}/{name}"; os.makedirs(out, exist_ok=True)
    t0 = time.monotonic()
    with get_task_stream() as ts:
        if fused:
            def write_part(df, pixel):
                df.to_parquet(f"{out}/Norder{pixel.order}_Npix{pixel.pixel}.parquet", index=False)
                return pd.DataFrame({"n": [len(df)]})
            counts = xm.map_partitions(write_part, include_pixel=True, meta=pd.DataFrame({"n": pd.Series(dtype="int64")}))
            rows = int(counts.compute()["n"].sum())
        else:
            xm.write_catalog(out, catalog_name="xmatch", as_collection=False, overwrite=True, progress_bar=False, error_if_empty=False)
            import hats; rows = int(hats.read_hats(out).catalog_info.total_rows)
    dt = time.monotonic() - t0
    time.sleep(1.5); stop.set(); th.join()
    a = np.array([s[1:] for s in samples], dtype=float); t = np.array([s[0] - t0 for s in samples])
    rss, proc_m, managed, unm_old, unm_rec, nkeys = (a[:, i] / 2**30 for i in range(5)) if False else (a[:,0]/2**30, a[:,1]/2**30, a[:,2]/2**30, a[:,3]/2**30, a[:,4]/2**30, a[:,5])
    i = rss.argmax()
    print(f"\n{name}: {rows:,} matched rows, {dt:.0f} s, {len(ts.data)} tasks in the stream | peak RSS {rss.max():.2f} GiB at t={t[i]:.0f} s: "
          f"managed {managed[i]:.2f}, unmanaged old {unm_old[i]:.2f}, recent {unm_rec[i]:.2f} GiB, {int(nkeys[i])} keys held", flush=True)
    # what ran around the peak: tasks overlapping [t_peak-10, t_peak+2]
    tp = t0 + t[i]
    running = [(d["key"] if isinstance(d["key"], str) else str(d["key"]))[:70] for d in ts.data if d["startstops"] and any(ss["start"] <= tp + 2 and ss["stop"] >= tp - 10 for ss in d["startstops"])]
    print("  tasks running within 10 s before the peak:", running, flush=True)
    # humps above 4 GiB: managed vs unmanaged there
    hi = np.where(rss > 4.0)[0]
    if len(hi):
        print(f"  samples > 4 GiB: {len(hi)} ({len(hi)*0.5:.0f} s); mean managed {managed[hi].mean():.2f}, unmanaged old {unm_old[hi].mean():.2f}, recent {unm_rec[hi].mean():.2f}, keys {nkeys[hi].mean():.1f}", flush=True)
    print("  max RSS per 60 s:", [round(rss[(t >= s) & (t < s + 60)].max(), 1) for s in range(0, int(t[-1]), 60) if ((t >= s) & (t < s + 60)).any()], flush=True)
    print("  max managed per 60 s:", [round(managed[(t >= s) & (t < s + 60)].max(), 1) for s in range(0, int(t[-1]), 60) if ((t >= s) & (t < s + 60)).any()], flush=True)
    kinds = pd.Series([str(d["key"]).split("-")[0].strip("(' ") for d in ts.data]).value_counts().to_dict()
    print("  task kinds:", kinds, flush=True)
    np.save(f"{out}/timeline.npy", np.column_stack([t, rss, managed, unm_old, unm_rec, nkeys]))
    json.dump([{"key": str(d["key"]), "start": d["startstops"][0]["start"] - t0, "stop": d["startstops"][-1]["stop"] - t0} for d in ts.data if d["startstops"]], open(f"{out}/tasks.json", "w"))
    client.close(); cluster.close()

def main():
    run_case("plane6_writecatalog", fused=False)
    run_case("plane6_fused", fused=True)
    print("DONE", flush=True)

if __name__ == "__main__":
    main()
