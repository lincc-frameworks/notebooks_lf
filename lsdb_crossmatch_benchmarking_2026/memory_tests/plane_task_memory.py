"""Peak worker RSS for single large Galactic-plane ZTF partitions crossmatched against Gaia (default columns vs RA/Dec),
and RSS drift over several such partitions on ONE worker. Answers: does one plane task fit in the 7.45 GiB worker budget?"""
import os, sys, time, threading, warnings, logging
warnings.filterwarnings("ignore"); logging.getLogger("distributed").setLevel(logging.ERROR)
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # the benchmark directory
import psutil, pandas as pd, numpy as np
import run_benchmark as rb
import hats
from lsdb import PixelSearch
from hats.pixel_math import HealpixPixel
from dask.distributed import Client, LocalCluster
OUT = os.environ.get("XMATCH_OUT_DIR", f"/sdf/data/rubin/user/{os.environ.get('USER', 'user')}/xmatch-bench-out") + "/plane-test"
DEF = {"ztf": rb.ZTF_DEFAULT_COLS, "gaia": rb.GAIA_DEFAULT_COLS}; RADEC = {"ztf": rb.RADEC_COLS, "gaia": rb.RADEC_COLS}

def run_case(name, pixels, columns, mem_gb=60):
    cluster = LocalCluster(n_workers=1, threads_per_worker=1, memory_limit=f"{mem_gb}GB", processes=True,
                           local_directory=f"/lscratch/{os.environ['USER']}/dask-plane-test", dashboard_address=":0")
    client = Client(cluster)
    procs = [psutil.Process(p) for p in client.run(os.getpid).values()]
    samples, stop = [], threading.Event()
    def poll():
        while not stop.is_set():
            try: samples.append((time.monotonic(), max(p.memory_info().rss for p in procs)))
            except psutil.Error: pass
            time.sleep(0.25)
    th = threading.Thread(target=poll, daemon=True); th.start()
    ztf, gaia = rb.open_catalogs(columns)
    sel = ztf.search(PixelSearch(pixels))
    gsel = gaia.search(PixelSearch(sel.get_healpix_pixels()))
    xm = sel.crossmatch(gsel, radius_arcsec=10, n_neighbors=1, suffixes=("_ztf", "_gaia"), suffix_method="all_columns")
    n_tasks, n_gaia = len(xm.get_healpix_pixels()), len(gsel.get_healpix_pixels())
    t0 = time.monotonic()
    xm.write_catalog(f"{OUT}/{name}", catalog_name="xmatch", as_collection=False, overwrite=True, progress_bar=False, error_if_empty=False)
    dt = time.monotonic() - t0
    time.sleep(1.5); stop.set(); th.join()
    rss = np.array([s[1] for s in samples]) / 2**30
    rows = hats.read_hats(f"{OUT}/{name}").catalog_info.total_rows
    print(f"{name}: {len(pixels)} ztf part -> {n_gaia} gaia part, {n_tasks} tasks, {rows:,} matched, {dt:.0f} s | worker RSS GiB: start {rss[0]:.2f} peak {rss.max():.2f} end {rss[-1]:.2f}", flush=True)
    client.close(); cluster.close()
    return rss

def main():
    pi = pd.read_csv("/sdf/data/rubin/cosmicdata/lsdb/ztf_dr23/objects/ztf_dr23_objects-hats/partition_info.csv")
    run_case("o4_921_default", [HealpixPixel(4, 921)], DEF)      # 10.87M rows, b = +5.6, 53 deg from the cone centre (inside a 3e9 cone)
    run_case("o4_921_radec", [HealpixPixel(4, 921)], RADEC)
    run_case("o3_634_default", [HealpixPixel(3, 634)], DEF)      # 10.87M rows, Order 3, b = +4.0 (full-catalog step only)
    # drift: the 6 largest partitions at |b| < 10 within 65 deg of the centre, one worker, sequential tasks
    import cdshealpix
    from astropy.coordinates import SkyCoord
    lon = np.empty(len(pi)); lat = np.empty(len(pi))
    for o in pi.Norder.unique():
        m = (pi.Norder == o).to_numpy(); l, b = cdshealpix.nested.healpix_to_lonlat(pi.Npix.to_numpy()[m].astype(np.uint64), int(o)); lon[m], lat[m] = l.deg, b.deg
    sc = SkyCoord(lon, lat, unit="deg"); pi["b"] = sc.galactic.b.deg; pi["sep"] = SkyCoord(216.8182, 57.3995, unit="deg").separation(sc).deg
    six = pi[(pi.b.abs() < 10) & (pi.sep < 65)].sort_values("num_rows", ascending=False).head(6)
    print("drift set:", [(int(o), int(p), int(n), round(b, 1)) for o, p, n, b in zip(six.Norder, six.Npix, six.num_rows, six.b)], flush=True)
    rss = run_case("plane6_default", [HealpixPixel(int(o), int(p)) for o, p in zip(six.Norder, six.Npix)], DEF)
    np.save(f"{OUT}/plane6_rss_gib.npy", rss)
    print("DONE", flush=True)

if __name__ == "__main__":
    main()
