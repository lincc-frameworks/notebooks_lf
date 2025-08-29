## AAVSO Variable Star Index (VSX) catalog

The VSX catalog is imported monthly:

```bash
SHELL=/bin/bash
CRON_TZ=UTC
@monthly /astro/users/smcampos/jobs/vsx/run_vsx.sh
```

We retain three catalogs at https://data.lsdb.io/hats/vsx. After this period, they expire and are removed.
