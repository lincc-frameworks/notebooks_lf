# Sprint demos for week of Feb 5, 2024

## Sandro

* [issue 90](https://github.com/astronomy-commons/lsdb/issues/90)

Potential demo:

    lsdb.read_hipscat("ztf")
        .box(ra=[],dec=[])
        .compute()

## Sean

* [issue 7](https://github.com/astronomy-commons/lsdb/issues/7)
* [issue 51](https://github.com/astronomy-commons/lsdb/issues/51)

Potential demo:

    gaia = lsdb.read_hipscat("gaia")
    ztf = lsdb.read_hipscat("ztf")
    vanilla_matches = ztf.crossmatch(gaia, r=1*arcsec).compute()

    ztf_margin = lsdb.read_hipscat("ztf_margin")
    ztf_with_margin = lsdb.read_hipscat("ztf", ztf_margin)
    margin_matches = ztf_with_margin.crossmatch(gaia, r=1*arcsec).compute()

    assert vanilla_matches != margin_matches

## Melissa

* [issue 107](https://github.com/astronomy-commons/lsdb/issues/107)
* [issue 108](https://github.com/astronomy-commons/lsdb/issues/108)

Demo: Show that the new stuff is faster than the old stuff.

## Mario

* [issue 105](https://github.com/astronomy-commons/lsdb/issues/105)
* [issue 188](https://github.com/astronomy-commons/hipscat/issues/188)
* [issue 196](https://github.com/astronomy-commons/hipscat-import/issues/196)

Demo: Can conda install hipscat/lsdb packages