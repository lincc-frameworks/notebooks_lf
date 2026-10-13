"""Compare a re-encoded HATS catalog/collection with its source: partition list, row count, columns, and the
full content of the first partition. Exit 1 on any difference.   usage: verify_copy.py <source> <copy>"""
import sys
import warnings

warnings.filterwarnings("ignore")
import lsdb

a, b = lsdb.open_catalog(sys.argv[1]), lsdb.open_catalog(sys.argv[2])
pa_, pb_ = a.get_healpix_pixels(), b.get_healpix_pixels()
na, nb = a.map_partitions(len).compute().sum(), b.map_partitions(len).compute().sum()
first = pa_[0]
da = a.pixel_search([(first.order, first.pixel)]).compute().sort_index()
db = b.pixel_search([(first.order, first.pixel)]).compute().sort_index()
checks = {"partitions": (len(pa_), len(pb_)), "rows": (int(na), int(nb)), "columns": (list(a.columns) == list(b.columns)),
          "margin": (a.margin is not None, b.margin is not None), "first_partition_identical": da.equals(db)}
ok = len(pa_) == len(pb_) and na == nb and checks["columns"] and checks["margin"][0] == checks["margin"][1] and checks["first_partition_identical"]
print({"result": "verify_copy", **checks, "ok": ok})
sys.exit(0 if ok else 1)
