Remember to record!!


## Kostya

- hug your face [NB](hf-s3-xmatch.ipynb)

## Olivia

## Doug

(incubator things)

## Sandro

- something PPDB - maybe the hats-import part of it.

## Sean

OoO

## Derek

### LSDB over TAP

- [TAP service](http://epyc.astro.washington.edu:43213) wrapping LSDB,
  - running on epyc, under my account
  - See the "Sample ADQL" below for what's being executed.
    - [Sample ADQL query](http://epyc.astro.washington.edu:43213/sync?REQUEST=doQuery&LANG=ADQL&QUERY=%0Aselect+top+5%0A++source_id%2C+ra%2C+dec%2C+phot_g_mean_mag%2C+pm%2C+parallax%2C+visibility_periods_used%0Afrom+gaia_dr3.gaia%0Awhere+1+%3D+contains%28point%28%27ICRS%27%2C+ra%2C+dec%29%2C+circle%28%27ICRS%27%2C+270%2C+23%2C+0.25%29%29%0Aand+phot_g_mean_mag+%3C+16%0A) demonstrates how it's done.
    - [Equivalent for Caltech](https://irsa.ipac.caltech.edu/TAP/sync?REQUEST=doQuery&LANG=ADQL&QUERY=%0Aselect+top+5%0A++source_id%2C+ra%2C+dec%2C+phot_g_mean_mag%2C+pm%2C+parallax%2C+visibility_periods_used%0Afrom+gaia_dr3_source%0Awhere+1+%3D+contains%28point%28%27ICRS%27%2C+ra%2C+dec%29%2C+circle%28%27ICRS%27%2C+270%2C+23%2C+0.25%29%29%0Aand+phot_g_mean_mag+%3C+15%0A) table is `gaia_dr3_source`; TAP server is at https://irsa.ipac.caltech.edu/TAP

```python
# How to form a synchronous TAP query
resp = requests.get(
    'http://epyc.astro.washington.edu:43213/sync',
    params={'REQUEST':'doQuery', 'LANG':'ADQL', 'QUERY': qstr}
    )
```

#### Sample ADQL

```sql
select top 5
  source_id, ra, dec, phot_g_mean_mag, pm, parallax, visibility_periods_used
from gaia_dr3.gaia
where 1 = contains(point('ICRS', ra, dc), circle('ICRS', 270, 23, 0.25))
and phot_g_mean_mag < 16
```

#### What's New Since Busy Week

##### Firefly Compatibility

- Firefly requires `TAP_SCHEMA` tables to exist and be queryable via
  ADQL to inspect all of the structure and schema.  In particular, the
  following query, and others, must be supported:

```sql
SELECT *
FROM tap_schema.schemas
INNER JOIN tap_schema.tables
ON tap_schema.tables.schema_name = tap_schema.schemas.schema_name
```

Added an SQLite3 DB to `tap_server.py` and a CLI tool to populate it
from another TAP service (e.g. the one at Caltech).

##### VO Metadata in ADQL Response

As you can see by the demo above, when an ADQL query is made against
the LSDB-over-TAP service, the payload now comes back with correct VO
metadata for the returned values.

### Left-Join Crossmatch Progress

- [left-join crossmatch demo](left-join-rows.ipynb) Last time, the correct pixels; today, the correct rows!

## Melissa

- VOParquet metadata generation.

# Seeking feedback

_If your demo will be long, or you want to have a discussion, please put your name at the end_
