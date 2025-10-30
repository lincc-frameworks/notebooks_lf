# TAP Server Prototype

A prototype implementation of a TAP (Table Access Protocol) server following the IVOA TAP specification v1.1.

## Overview

This TAP server prototype accepts ADQL (Astronomical Data Query Language) queries and returns results in VOTable format. The server is built using Flask and integrates with the `bin/adql_to_lsdb` module to convert ADQL queries to LSDB-compatible operations.

**NEW**: TAP_SCHEMA metadata is now stored in a SQLite database, providing native SQL query support including JOINs, WHERE clauses, and aggregations. The database uses Python's built-in `sqlite3` module with no external dependencies.

Components:
- **TAP Schema Database** (`tap_schema_db.py`): SQLite-based storage for TAP_SCHEMA metadata
- **TAP Schema Initialization** (`tap_schema_init.py`): Script to create and populate the metadata database
- **TAP Schema Import** (`import_tap_schema.py`): CLI tool to import metadata from external TAP servers
- **ADQL to LSDB Converter** (`../bin/adql_to_lsdb.py`): Comprehensive ADQL parser using queryparser library
- **TAP Server** (`tap_server.py`): Flask-based HTTP server implementing TAP protocol endpoints

## Features

### Implemented
- ✅ Synchronous query endpoint (`/sync`)
- ✅ **TAP_SCHEMA Support** - SQLite-based storage with native SQL query execution
  - `TAP_SCHEMA.schemas` - List of available schemas
  - `TAP_SCHEMA.tables` - List of available tables
  - `TAP_SCHEMA.columns` - List of all columns with metadata
  - `TAP_SCHEMA.keys` - Foreign key relationships
  - `TAP_SCHEMA.key_columns` - Foreign key column mappings
  - Native SQL support for JOINs, WHERE, ORDER BY, GROUP BY, and aggregations
  - Database inspectable with standard SQLite tools (CLI, DB Browser, etc.)
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
pip install pyvo  # Required for importing from external TAP servers
```

2. Initialize the TAP_SCHEMA database:
```bash
python tap_schema_init.py
```

This will create `tap_schema.db` with sample metadata for the TAP service.

3. (Optional) Import real catalog metadata from external TAP servers:
```bash
# Import Gaia DR3 from ESA Gaia TAP server
python import_tap_schema.py --url https://gea.esac.esa.int/tap-server/tap --schema gaiadr3

# Import WISE AllWISE from IRSA
python import_tap_schema.py --url https://irsa.ipac.caltech.edu/TAP --schema wise_allwise
```

See the [TAP Schema Import Tool](#tap-schema-import-tool) section for more details.

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

#### Querying Catalog Data

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

#### Querying TAP_SCHEMA Metadata

List all available schemas:
```sql
SELECT * FROM TAP_SCHEMA.schemas
```

List all tables in a schema:
```sql
SELECT table_name, description 
FROM TAP_SCHEMA.tables 
WHERE schema_name = 'public'
```

Get column metadata for a specific table:
```sql
SELECT column_name, datatype, unit, ucd, description
FROM TAP_SCHEMA.columns
WHERE table_name = 'public.ztf_dr14'
```

Find all indexed columns:
```sql
SELECT table_name, column_name 
FROM TAP_SCHEMA.columns 
WHERE indexed = 1
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

## TAP_SCHEMA

This server implements the TAP_SCHEMA metadata as defined in section 4.3 of the IVOA TAP specification. TAP_SCHEMA is a set of special tables that describe the schema of the TAP service itself.

**Storage Implementation**: TAP_SCHEMA data is stored in a SQLite database (`tap_schema.db`) using Python's built-in `sqlite3` module. This provides:
- Native SQL query execution (no custom query logic needed)
- Support for complex queries including JOINs, WHERE, ORDER BY, GROUP BY
- Database inspectable with standard SQLite tools (CLI: `sqlite3`, GUI: DB Browser for SQLite)
- No external dependencies beyond Python's standard library
- Easy to extend for future requirements

### Database Management

#### Initialize/Recreate the Database
```bash
python tap_schema_init.py [--db-path tap_schema.db] [--clear]
```

The `--clear` flag will delete existing data before repopulating.

#### Inspect the Database with SQLite CLI
```bash
# Open the database
sqlite3 tap_schema.db

# List all tables
.tables

# View schema structure
.schema schemas

# Query data
SELECT * FROM schemas;

# Execute JOINs
SELECT * FROM schemas 
INNER JOIN tables ON tables.schema_name = schemas.schema_name;

# Exit
.quit
```

#### Using the Database in Python
```python
from tap_schema_db import TAPSchemaDatabase

# Connect to database
with TAPSchemaDatabase('tap_schema.db') as db:
    # Simple query
    results = db.query("SELECT * FROM schemas")
    
    # Query with JOIN
    results = db.query("""
        SELECT * FROM schemas 
        INNER JOIN tables ON tables.schema_name = schemas.schema_name
        WHERE schemas.schema_name = ?
    """, ('public',))
    
    # Get results with column names
    data, columns = db.query_with_columns("SELECT * FROM tables")
```

See `example_join_query.py` for more examples of JOIN queries and complex SQL operations.

### TAP_SCHEMA Tables

The following TAP_SCHEMA tables are queryable via ADQL or SQL:

#### TAP_SCHEMA.schemas
Lists all schemas available in this TAP service.
- `schema_name` - Name of the schema
- `description` - Description of the schema
- `utype` - UType for the schema

#### TAP_SCHEMA.tables
Lists all tables available in this TAP service.
- `schema_name` - Schema containing the table
- `table_name` - Name of the table
- `table_type` - Type (usually 'table' or 'view')
- `description` - Description of the table
- `utype` - UType for the table

#### TAP_SCHEMA.columns
Lists all columns in all tables with detailed metadata.
- `table_name` - Fully qualified table name (schema.table)
- `column_name` - Name of the column
- `description` - Description of the column
- `unit` - Unit of measure
- `ucd` - Unified Content Descriptor
- `utype` - UType for the column
- `datatype` - ADQL datatype (e.g., 'double', 'char', 'long')
- `size` - Size for variable-length types
- `principal` - 1 if this is a principal column, 0 otherwise
- `indexed` - 1 if this column is indexed, 0 otherwise
- `std` - 1 if defined by a standard, 0 otherwise

#### TAP_SCHEMA.keys
Lists foreign key relationships between tables.
- `key_id` - Unique identifier for the key
- `from_table` - Source table name
- `target_table` - Target table name
- `description` - Description of the relationship
- `utype` - UType for the key

#### TAP_SCHEMA.key_columns
Lists the columns that participate in foreign keys.
- `key_id` - Foreign key identifier
- `from_column` - Column name in source table
- `target_column` - Column name in target table

### Querying TAP_SCHEMA

TAP_SCHEMA tables can be queried just like any other tables using ADQL:

```bash
# Get all schemas
curl -X POST http://localhost:5000/sync \
  -d "REQUEST=doQuery" \
  -d "LANG=ADQL" \
  -d "QUERY=SELECT * FROM TAP_SCHEMA.schemas"

# Get tables in a specific schema
curl -X POST http://localhost:5000/sync \
  -d "REQUEST=doQuery" \
  -d "LANG=ADQL" \
  -d "QUERY=SELECT table_name FROM TAP_SCHEMA.tables WHERE schema_name='public'"

# Get column details for a specific table
curl -X POST http://localhost:5000/sync \
  -d "REQUEST=doQuery" \
  -d "LANG=ADQL" \
  -d "QUERY=SELECT column_name, datatype, unit FROM TAP_SCHEMA.columns WHERE table_name='public.ztf_dr14'"
```

## TAP Schema Import Tool

The `import_tap_schema.py` tool allows you to populate your local TAP_SCHEMA database with real catalog metadata from external TAP servers. This enables your TAP service to provide accurate schema information for astronomical catalogs without manually entering the metadata.

### Features

- Query external TAP servers for schema metadata
- Import complete catalog schemas including tables and columns
- Support for foreign key relationships
- Graceful error handling for network issues and incomplete metadata
- Verbose logging for debugging
- Compatible with any IVOA TAP 1.1 compliant server

### Usage

#### Basic Import

Import a catalog schema from an external TAP server:

```bash
python import_tap_schema.py --url <TAP_URL> --schema <schema_name>
```

#### Examples

Import Gaia DR3 from ESA Gaia TAP server:
```bash
python import_tap_schema.py --url https://gea.esac.esa.int/tap-server/tap --schema gaiadr3
```

Import WISE AllWISE catalog from IRSA:
```bash
python import_tap_schema.py --url https://irsa.ipac.caltech.edu/TAP --schema wise_allwise
```

Import a specific table by name:
```bash
python import_tap_schema.py --url https://irsa.ipac.caltech.edu/TAP --table gaia_dr3_source
```

Import to a custom database location:
```bash
python import_tap_schema.py --url <TAP_URL> --schema <schema> --db-path /path/to/my_db.sqlite
```

Enable verbose logging:
```bash
python import_tap_schema.py --url <TAP_URL> --table <table> --verbose
```

Skip foreign key import (faster):
```bash
python import_tap_schema.py --url <TAP_URL> --schema <schema> --no-keys
```

#### Command-Line Options

- `--url <URL>` (required): URL of the external TAP server
- `--schema <name>`: Name of the schema/catalog to import (mutually exclusive with `--table`)
- `--table <name>`: Name of a specific table to import (mutually exclusive with `--schema`)
- `--db-path <path>`: Path to local SQLite database (default: `tap_schema.db`)
- `--no-keys`: Skip importing foreign key relationships
- `--verbose` or `-v`: Enable verbose debug logging

**Note**: You must specify either `--schema` to import an entire schema or `--table` to import a specific table, but not both.

#### Programmatic Usage

You can also use the import tool programmatically:

```python
from import_tap_schema import TAPSchemaImporter
from tap_schema_db import TAPSchemaDatabase

# Import entire schema metadata
tap_url = 'https://gea.esac.esa.int/tap-server/tap'
with TAPSchemaImporter(tap_url, 'my_catalog.db') as importer:
    importer.import_schema_metadata('gaiadr3', include_keys=True)

# Or import a specific table by name
with TAPSchemaImporter(tap_url, 'my_catalog.db') as importer:
    importer.import_table_by_name('gaia_dr3_source', include_keys=True)

# Query the imported metadata
with TAPSchemaDatabase('my_catalog.db') as db:
    tables = db.query("SELECT * FROM tables WHERE schema_name = 'gaiadr3'")
    for table in tables:
        print(f"Table: {table['table_name']}")
```

### Workflow Example

1. **Import catalog metadata** from an external TAP server:
   ```bash
   python import_tap_schema.py --url https://irsa.ipac.caltech.edu/TAP --schema gaiadr3
   ```

2. **Inspect the imported metadata** with SQLite:
   ```bash
   sqlite3 tap_schema.db
   sqlite> SELECT * FROM tables WHERE schema_name = 'gaiadr3';
   sqlite> SELECT column_name, datatype, unit FROM columns WHERE table_name = 'gaiadr3.gaia_source' LIMIT 10;
   ```

3. **Start your TAP server** to serve the metadata:
   ```bash
   python tap_server.py
   ```

4. **Query the metadata** via TAP protocol:
   ```bash
   curl -X POST http://localhost:5000/sync \
     -d "REQUEST=doQuery" \
     -d "LANG=ADQL" \
     -d "QUERY=SELECT * FROM TAP_SCHEMA.tables WHERE schema_name='gaiadr3'"
   ```

### Supported TAP Services

The import tool works with any IVOA TAP 1.1 compliant service, including:

- **ESA Gaia Archive**: https://gea.esac.esa.int/tap-server/tap
- **IRSA (Caltech)**: https://irsa.ipac.caltech.edu/TAP
- **CADC**: https://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/tap
- **GAVO Data Center**: https://dc.zah.uni-heidelberg.de/tap
- Many other astronomical data archives

### Error Handling

The tool handles common issues gracefully:

- **Network errors**: Connection failures are reported with clear error messages
- **Missing schemas**: If a schema is not found, the tool will attempt to import tables anyway
- **Incomplete metadata**: Missing fields are handled as NULL values
- **Foreign key errors**: If the TAP server doesn't support foreign keys, the import continues without them

For detailed error information, use the `--verbose` flag.

### Testing

Run the test suite to verify the import tool:

```bash
python -m unittest test_import_tap_schema -v
```

See `example_import_usage.py` for more detailed examples.

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
- **Section 4.3**: TAP_SCHEMA metadata tables implementation

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
         ├──→ TAP_SCHEMA query?
         │    Yes: ↓
         │    ┌──────────────────┐
         │    │ TAP Schema DB    │
         │    │ (SQLite)         │
         │    │ - Native SQL     │
         │    │ - JOINs, WHERE   │
         │    └──────────────────┘
         │    No: ↓
         ↓
┌─────────────────┐
│  ADQL Parser    │
│(adql_to_lsdb.py)│
└────────┬────────┘
         │
         ↓
┌─────────────────┐
│  LSDB Catalog   │
│  or Sample Data │
└─────────────────┘
```

## Module Reference

### `tap_schema_db.py`
Database operations module for TAP_SCHEMA metadata storage.

**Key Classes:**
- `TAPSchemaDatabase`: Main database operations class

**Key Methods:**
- `initialize_schema()`: Create TAP_SCHEMA tables
- `insert_schema()`, `insert_table()`, `insert_column()`: Add metadata
- `query()`: Execute arbitrary SQL queries
- `query_with_columns()`: Execute SQL and get column names

**Usage:**
```python
from tap_schema_db import TAPSchemaDatabase

with TAPSchemaDatabase('tap_schema.db') as db:
    db.initialize_schema()
    db.insert_schema('myschema', 'My Schema')
    results = db.query("SELECT * FROM schemas")
```

### `tap_schema_init.py`
Initialization script to create and populate the TAP_SCHEMA database.

**Usage:**
```bash
python tap_schema_init.py [--db-path tap_schema.db] [--clear]
```

**Options:**
- `--db-path`: Path to database file (default: tap_schema.db)
- `--clear`: Clear existing data before populating

### `example_join_query.py`
Example script demonstrating SQL JOIN queries on TAP_SCHEMA.

**Usage:**
```bash
python example_join_query.py [--db-path tap_schema.db]
```

**Examples Included:**
1. Basic JOIN between schemas and tables
2. JOIN with WHERE clause filtering
3. Simple queries without JOINs
4. Three-way JOIN across schemas, tables, and columns

## Future Work

This prototype provides a foundation for:

1. **Real Query Execution**: Integration with LSDB to execute queries against HiPSCat catalogs
2. **Asynchronous Queries**: Implement `/async` endpoint for long-running queries
3. **Additional Formats**: Support for CSV, JSON, FITS output formats
4. **Advanced ADQL**: Support for JOINs, subqueries, geometric functions (catalog JOINs, not TAP_SCHEMA JOINs)
5. **Performance**: Query optimization and result caching
6. **Security**: Authentication, rate limiting, and query validation
7. **Metadata Import**: Tools to import metadata from LSDB catalogs into TAP_SCHEMA database

## References

- [IVOA TAP Specification v1.1](https://www.ivoa.net/documents/TAP/20181024/PR-TAP-1.1-20181024.html)
- [ADQL Language Specification](https://www.ivoa.net/documents/ADQL/)
- [VOTable Format Specification](https://www.ivoa.net/documents/VOTable/)
- [LSDB Documentation](https://lsdb.readthedocs.io/)

## License

This prototype is part of the LINCC Frameworks notebooks repository.
