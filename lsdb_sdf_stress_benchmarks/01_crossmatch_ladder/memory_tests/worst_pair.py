"""Over the WHOLE ZTF DR23 x Gaia DR3 alignment: which aligned piece of sky carries the most rows into one crossmatch task,
and how much memory does that task take (one worker, default columns)."""
import os, sys, time, threading, warnings, logging, re
warnings.filterwarnings("ignore"); logging.getLogger("distributed").setLevel(logging.ERROR)
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # the benchmark directory
import numpy as np, pandas as pd, pyarrow.parquet as pq, psutil
import hats, run_benchmark as rb
from hats.pixel_tree.pixel_alignment import align_trees
from hats.pixel_math import HealpixPixel
ZTF = "/sdf/data/rubin/cosmicdata/lsdb/ztf_dr23/objects/ztf_dr23_objects-hats"
GAIA = "/sdf/data/rubin/cosmicdata/lsdb/gaia_dr3/gaia"
OUT = os.environ.get("XMATCH_OUT_DIR", f"/sdf/data/rubin/user/{os.environ.get('USER', 'user')}/xmatch-bench-out") + "/plane-test"

def rows_per_partition(path):
    md = pq.read_metadata(f"{path}/dataset/_metadata"); rows = {}
    for i in range(md.num_row_groups):
        rg = md.row_group(i); f = rg.column(0).file_path
        m = re.search(r"Norder=(\d+)/.*Npix=(\d+)", f); rows[(int(m.group(1)), int(m.group(2)))] = rows.get((int(m.group(1)), int(m.group(2))), 0) + rg.num_rows
    return pd.Series(rows)

def main():
    zc, gc = hats.read_hats(ZTF), hats.read_hats(GAIA)
    zrows, grows = rows_per_partition(ZTF), rows_per_partition(GAIA)
    print(f"ZTF {len(zrows)} partitions, max {zrows.max():,} rows; Gaia {len(grows)} partitions, max {grows.max():,} rows, median {int(grows.median()):,}", flush=True)
    al = align_trees(zc.pixel_tree, gc.pixel_tree, alignment_type="inner").pixel_mapping
    al.columns = [c.lower() for c in al.columns]
    z = list(zip(al.primary_norder, al.primary_npix)); g = list(zip(al.join_norder, al.join_npix))
    al["ztf_rows_part"] = zrows.reindex(z).to_numpy(); al["gaia_rows_part"] = grows.reindex(g).to_numpy()
    # rows of each partition that fall in the aligned piece: all of them if the partition IS the aligned pixel, else the pixel-area fraction
    al["ztf_rows_piece"] = al.ztf_rows_part / 4.0 ** (al.aligned_norder - al.primary_norder)
    al["gaia_rows_piece"] = al.gaia_rows_part / 4.0 ** (al.aligned_norder - al.join_norder)
    al["task_rows"] = al.ztf_rows_piece + al.gaia_rows_piece
    print(f"{len(al)} aligned pieces (tasks) in the full crossmatch", flush=True)
    top = al.sort_values("task_rows", ascending=False).head(8)
    print("\nlargest tasks by rows entering the task (ZTF piece + Gaia piece; pieces = whole partition or area fraction):")
    print(f"{'ZTF part':>9} {'ZTF rows':>11} {'Gaia part':>10} {'Gaia rows':>10} {'aligned':>8} {'ZTF piece':>10} {'Gaia piece':>10} {'|b|':>5}")
    import cdshealpix; from astropy.coordinates import SkyCoord
    for r in top.itertuples():
        lon, lat = cdshealpix.nested.healpix_to_lonlat(np.array([r.aligned_npix], dtype=np.uint64), int(r.aligned_norder))
        b = SkyCoord(lon.deg, lat.deg, unit="deg").galactic.b.deg[0]
        print(f"{str(r.primary_norder)+'/'+str(r.primary_npix):>9} {int(r.ztf_rows_part):>11,} {str(r.join_norder)+'/'+str(r.join_npix):>10} {int(r.gaia_rows_part):>10,} "
              f"{str(r.aligned_norder)+'/'+str(r.aligned_npix):>8} {int(r.ztf_rows_piece):>10,} {int(r.gaia_rows_piece):>10,} {b:5.1f}", flush=True)
    worst = top.iloc[0]
    # also the largest ZTF piece alone (result size ~ matched rows <= ZTF piece rows)
    wz = al.sort_values("ztf_rows_piece", ascending=False).iloc[0]
    print(f"\nlargest ZTF piece: ZTF {wz.primary_norder}/{wz.primary_npix} ({int(wz.ztf_rows_piece):,} rows) with Gaia {wz.join_norder}/{wz.join_npix} ({int(wz.gaia_rows_piece):,} rows in the piece)", flush=True)

    # ---- measure the worst task on one worker
    from dask.distributed import Client, LocalCluster
    from lsdb import PixelSearch
    import psutil
    cases = {("worst_by_total", int(worst.primary_norder), int(worst.primary_npix), int(worst.join_norder), int(worst.join_npix)),
             ("worst_by_ztf", int(wz.primary_norder), int(wz.primary_npix), int(wz.join_norder), int(wz.join_npix))}
    for name, zo, zp, go, gp_ in sorted(cases):
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
        sel = ztf.search(PixelSearch([HealpixPixel(zo, zp)])); gsel = gaia.search(PixelSearch([HealpixPixel(go, gp_)]))
        xm = sel.crossmatch(gsel, radius_arcsec=10, n_neighbors=1, suffixes=("_ztf", "_gaia"), suffix_method="all_columns")
        n_tasks = len(xm.get_healpix_pixels()); t0 = time.monotonic()
        out = f"{OUT}/{name}_{zo}_{zp}"
        xm.write_catalog(out, catalog_name="xmatch", as_collection=False, overwrite=True, progress_bar=False, error_if_empty=False)
        dt = time.monotonic() - t0; time.sleep(1.5); stop.set(); th.join()
        rows = hats.read_hats(out).catalog_info.total_rows
        print(f"\nMEASURED {name}: ZTF {zo}/{zp} x Gaia {go}/{gp_}: {n_tasks} task(s), {rows:,} matched rows, {dt:.0f} s, peak worker RSS {max(samples)/2**30:.2f} GiB", flush=True)
        client.close(); cluster.close()
    print("DONE", flush=True)

if __name__ == "__main__":
    main()
