"""Peak one-worker RSS for given (ZTF pixel, Gaia pixel) crossmatch tasks, default columns. usage: measure_pairs.py zo/zp:go/gp ..."""
import os, sys, time, threading, warnings, logging
warnings.filterwarnings("ignore"); logging.getLogger("distributed").setLevel(logging.ERROR)
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # the benchmark directory
import psutil, hats, run_benchmark as rb
from hats.pixel_math import HealpixPixel
from lsdb import PixelSearch
from dask.distributed import Client, LocalCluster
OUT = os.environ.get("XMATCH_OUT_DIR", f"/sdf/data/rubin/user/{os.environ.get('USER', 'user')}/xmatch-bench-out") + "/plane-test"

def main():
    for arg in sys.argv[1:]:
        z, g = arg.split(":"); zo, zp = map(int, z.split("/")); go, gp = map(int, g.split("/"))
        cluster = LocalCluster(n_workers=1, threads_per_worker=1, memory_limit="60GB", processes=True,
                               local_directory=f"/lscratch/{os.environ['USER']}/dask-plane-test", dashboard_address=":0")
        client = Client(cluster); proc = psutil.Process(next(iter(client.run(os.getpid).values())))
        samples, stop = [], threading.Event()
        def poll():
            while not stop.is_set():
                try: samples.append(proc.memory_info().rss)
                except psutil.Error: pass
                time.sleep(0.25)
        th = threading.Thread(target=poll, daemon=True); th.start()
        ztf, gaia = rb.open_catalogs({"ztf": rb.ZTF_DEFAULT_COLS, "gaia": rb.GAIA_DEFAULT_COLS})
        sel = ztf.search(PixelSearch([HealpixPixel(zo, zp)])); gsel = gaia.search(PixelSearch([HealpixPixel(go, gp)]))
        xm = sel.crossmatch(gsel, radius_arcsec=10, n_neighbors=1, suffixes=("_ztf", "_gaia"), suffix_method="all_columns")
        n_tasks = len(xm.get_healpix_pixels()); t0 = time.monotonic(); out = f"{OUT}/pair_{zo}_{zp}"
        xm.write_catalog(out, catalog_name="xmatch", as_collection=False, overwrite=True, progress_bar=False, error_if_empty=False)
        dt = time.monotonic() - t0; time.sleep(1.5); stop.set(); th.join()
        rows = hats.read_hats(out).catalog_info.total_rows
        print(f"MEASURED ZTF {zo}/{zp} x Gaia {go}/{gp}: {n_tasks} task(s), {rows:,} matched rows, {dt:.0f} s, peak worker RSS {max(samples)/2**30:.2f} GiB", flush=True)
        client.close(); cluster.close()
    print("DONE", flush=True)

if __name__ == "__main__":
    main()
