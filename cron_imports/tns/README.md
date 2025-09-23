## Transient Name Server (TNS) catalog

The TNS catalog is imported daily, 1 hour after UT midnight:

```bash
SHELL=/bin/bash
CRON_TZ=UTC
0 1 * * * /astro/users/smcampos/jobs/tns/run_tns.sh
```

They are retained for three months at https://data.lsdb.io/hats/tns. After this period, they expire and are removed.
