# TAP Server Prototype

A prototype implementation of a TAP (Table Access Protocol) server following the IVOA TAP specification v1.1.

## Overview

This TAP server prototype accepts ADQL (Astronomical Data Query Language) queries and returns results in VOTable format. The server is built using Flask and integrates with the `bin/adql_to_lsdb` module to convert ADQL queries to LSDB-compatible operations.

Components:
- **ADQL to LSDB Converter** (`../bin/adql_to_lsdb.py`): Comprehensive ADQL parser using queryparser library
- **TAP Server** (`tap_server.py`): Flask-based HTTP server implementing TAP protocol endpoints

## Features

### Implemented
- ✅ Synchronous query endpoint (`/sync`)
- ✅ Advanced ADQL query parsing using queryparser library
  - SELECT with column list or *
  - FROM with table names (including schema.table)
  - WHERE clause with comparison operators
  - CONTAINS with POINT and CIRCLE for cone searches
  - TOP/LIMIT clauses
- ✅ ADQL to LSDB code generation
- ✅ VOTable XML response format
- ✅ Service capabilities endpoint (`/capabilities`)
- ✅ Tables metadata endpoint (`/tables`)
- ✅ Error handling with VOTable error responses
- ✅ Sample data generation (for testing without actual LSDB catalogs)

### Not Yet Implemented
- ⏳ Actual query execution against LSDB catalogs (currently returns sample data)
- ⏳ Asynchronous query support (`/async`)
- ⏳ Additional output formats (CSV, JSON, FITS)
- ⏳ Authentication and authorization
- ⏳ Query result storage and retrieval

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Starting the Server

Run the server:
```bash
python tap_server.py
```

The server will start on `http://localhost:5000`

### Making Queries

#### Using curl (POST request):
```bash
curl -X POST http://localhost:5000/sync \
  -d "REQUEST=doQuery" \
  -d "LANG=ADQL" \
  -d "QUERY=SELECT TOP 10 ra, dec, mag FROM ztf_dr14 WHERE mag < 20"
```

#### Cone Search Query:
```bash
curl -X POST http://localhost:5000/sync \
  -d "REQUEST=doQuery" \
  -d "LANG=ADQL" \
  -d "QUERY=SELECT TOP 15 source_id, ra, dec, phot_g_mean_mag FROM gaia_dr3.gaia WHERE 1 = CONTAINS(POINT('ICRS', 270.0, 23.0), CIRCLE('ICRS', 270.0, 23.0, 0.25))"
```

#### Using curl (GET request):
```bash
curl "http://localhost:5000/sync?REQUEST=doQuery&LANG=ADQL&QUERY=SELECT%20TOP%205%20*%20FROM%20ztf_dr14"
```

#### Using Python requests:
```python
import requests

url = 'http://localhost:5000/sync'
params = {
    'REQUEST': 'doQuery',
    'LANG': 'ADQL',
    'QUERY': 'SELECT TOP 10 ra, dec, mag FROM ztf_dr14 WHERE mag < 20'
}

response = requests.post(url, data=params)
print(response.text)
```

### Available Endpoints

- **`GET /`** - Server information and documentation
- **`GET/POST /sync`** - Synchronous query endpoint
- **`GET /capabilities`** - Service capabilities in VOSI format
- **`GET /tables`** - Available tables and their schemas

### ADQL Query Examples

Select all columns with TOP:
```sql
SELECT TOP 10 * FROM ztf_dr14
```

Select specific columns:
```sql
SELECT TOP 100 ra, dec, mag FROM ztf_dr14
```

With WHERE clause:
```sql
SELECT TOP 50 ra, dec, mag FROM ztf_dr14 WHERE mag < 20
```

Cone search with spatial constraint:
```sql
SELECT TOP 15 source_id, ra, dec, phot_g_mean_mag 
FROM gaia_dr3.gaia 
WHERE 1 = CONTAINS(
    POINT('ICRS', 270.0, 23.0),
    CIRCLE('ICRS', 270.0, 23.0, 0.25)
)
```

## ADQL to LSDB Conversion

The TAP server uses the `bin/adql_to_lsdb` module to convert ADQL queries to LSDB Python code. This module can also be used standalone:

```bash
cd ../bin
python adql_to_lsdb.py < sample.adql
```

Or from Python:
```python
import sys
sys.path.insert(0, '../bin')
from adql_to_lsdb import adql_to_lsdb

query = "SELECT TOP 15 source_id, ra, dec FROM gaia_dr3.gaia WHERE phot_g_mean_mag < 16"
lsdb_code = adql_to_lsdb(query)
print(lsdb_code)
```

The generated code will look like:
```python
import lsdb

cat = lsdb.open_catalog(
    'https://data.lsdb.io/hats/gaia_dr3/gaia/',
    columns=[
        "source_id", "ra", "dec"
    ],
    filters=[('phot_g_mean_mag', '<', 16)],
    )

result = cat.head(15)
```

## Response Format

The server returns results in VOTable format (XML). Example response:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<VOTABLE version="1.4" xmlns="http://www.ivoa.net/xml/VOTable/v1.4">
  <RESOURCE type="results">
    <INFO name="QUERY_STATUS" value="OK"/>
    <INFO name="QUERY" value="SELECT ra, dec, mag FROM ztf_dr14 LIMIT 3"/>
    <TABLE name="ztf_dr14">
      <FIELD name="ra" datatype="double" unit="deg" ucd="pos.eq.ra;meta.main"/>
      <FIELD name="dec" datatype="double" unit="deg" ucd="pos.eq.dec;meta.main"/>
      <FIELD name="mag" datatype="double" unit="mag" ucd="phot.mag"/>
      <DATA>
        <TABLEDATA>
          <TR><TD>180.0</TD><TD>-30.0</TD><TD>15.0</TD></TR>
          <TR><TD>180.1</TD><TD>-29.9</TD><TD>15.5</TD></TR>
          <TR><TD>180.2</TD><TD>-29.8</TD><TD>16.0</TD></TR>
        </TABLEDATA>
      </DATA>
    </TABLE>
  </RESOURCE>
</VOTABLE>
```

## TAP Protocol Compliance

This prototype implements the following TAP v1.1 features:

- **Section 2.1**: Basic TAP service structure
- **Section 2.2**: Synchronous query execution via `/sync`
- **Section 2.5**: ADQL query language support
- **Section 2.7**: VOTable output format
- **Section 2.8**: Error reporting via VOTable INFO elements

## Architecture

```
┌─────────────────┐
│   HTTP Client   │
└────────┬────────┘
         │ ADQL Query
         ↓
┌─────────────────┐
│  TAP Server     │
│  (tap_server.py)│
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│  ADQL Parser    │
│(adql_to_lsdb.py)│
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│  Sample Data    │
│   Generator     │
└─────────────────┘
```

## Future Work

This prototype provides a foundation for:

1. **Real Query Execution**: Integration with LSDB to execute queries against HiPSCat catalogs
2. **Asynchronous Queries**: Implement `/async` endpoint for long-running queries
3. **Additional Formats**: Support for CSV, JSON, FITS output formats
4. **Advanced ADQL**: Support for JOINs, subqueries, geometric functions
5. **Performance**: Query optimization and result caching
6. **Security**: Authentication, rate limiting, and query validation

## References

- [IVOA TAP Specification v1.1](https://www.ivoa.net/documents/TAP/20181024/PR-TAP-1.1-20181024.html)
- [ADQL Language Specification](https://www.ivoa.net/documents/ADQL/)
- [VOTable Format Specification](https://www.ivoa.net/documents/VOTable/)
- [LSDB Documentation](https://lsdb.readthedocs.io/)

## License

This prototype is part of the LINCC Frameworks notebooks repository.
