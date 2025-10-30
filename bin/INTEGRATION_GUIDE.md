# TAP Schema Import Integration Guide

This guide explains how to use the `import_tap_schema.py` tool to populate a TAP schema database and integrate it with a TAP server implementation.

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Import Schema Metadata

Import schema from an external TAP server:

```bash
# Import Gaia DR3 from IRSA
./import_tap_schema.py --url https://irsa.ipac.caltech.edu/TAP --catalog gaiadr3

# Import from ESA Gaia TAP service
./import_tap_schema.py --url https://gea.esac.esa.int/tap-server/tap --schema gaiadr3

# Import a specific table
./import_tap_schema.py --url https://irsa.ipac.caltech.edu/TAP --table gaiadr3.gaia_source
```

This creates a `tap_schema.db` SQLite database in the current directory.

### 3. Inspect the Database

```bash
# View tables
sqlite3 tap_schema.db "SELECT * FROM tables;"

# View schemas
sqlite3 tap_schema.db "SELECT * FROM schemas;"

# View columns for a specific table
sqlite3 tap_schema.db "SELECT column_name, datatype, unit, description FROM columns WHERE table_name = 'gaiadr3.gaia_source' LIMIT 10;"

# Test the JOIN query from Issue #45
sqlite3 tap_schema.db "SELECT s.schema_name, t.table_name FROM schemas s INNER JOIN tables t ON s.schema_name = t.schema_name;"
```

## Integration with TAP Server

### Database Attachment Pattern (Issue #45)

As specified in Issue #45, the TAP server should use the ATTACH pattern:

```python
import sqlite3
import os

# Path to the TAP schema database
TAP_SCHEMA_DB = os.path.abspath("tap_schema.db")

# Main database connection
conn = sqlite3.connect("main.db")
cur = conn.cursor()

# Attach the TAP schema database at runtime
cur.execute("ATTACH DATABASE ? AS tap_schema", (TAP_SCHEMA_DB,))

# Now TAP queries work natively
result = cur.execute("""
    SELECT * 
    FROM tap_schema.schemas 
    INNER JOIN tap_schema.tables 
    ON tap_schema.tables.schema_name = tap_schema.schemas.schema_name
""")

for row in result:
    print(row)

conn.close()
```

### Example TAP Server Handler

```python
import sqlite3
from typing import Optional

class TAPSchemaHandler:
    """Handle TAP schema queries using the populated SQLite database."""
    
    def __init__(self, tap_schema_db_path: str = "tap_schema.db"):
        self.tap_schema_db = os.path.abspath(tap_schema_db_path)
        
    def execute_tap_query(self, query: str):
        """Execute a TAP query with the schema database attached."""
        conn = sqlite3.connect(":memory:")  # or your main database
        cur = conn.cursor()
        
        # Attach TAP schema database
        cur.execute("ATTACH DATABASE ? AS tap_schema", (self.tap_schema_db,))
        
        # Execute the query
        result = cur.execute(query)
        rows = result.fetchall()
        
        conn.close()
        return rows
    
    def get_schemas(self) -> list:
        """Get all schemas."""
        query = "SELECT schema_name, schema_description FROM tap_schema.schemas"
        return self.execute_tap_query(query)
    
    def get_tables(self, schema_name: Optional[str] = None) -> list:
        """Get tables, optionally filtered by schema."""
        if schema_name:
            # Use parameterized query for security
            conn = sqlite3.connect(":memory:")
            cur = conn.cursor()
            cur.execute("ATTACH DATABASE ? AS tap_schema", (self.tap_schema_db,))
            result = cur.execute(
                "SELECT table_name, schema_name, table_description FROM tap_schema.tables WHERE schema_name = ?",
                (schema_name,)
            )
            rows = result.fetchall()
            conn.close()
            return rows
        else:
            query = "SELECT table_name, schema_name, table_description FROM tap_schema.tables"
            return self.execute_tap_query(query)
    
    def get_columns(self, table_name: str) -> list:
        """Get columns for a specific table."""
        # Use parameterized query for security
        conn = sqlite3.connect(":memory:")
        cur = conn.cursor()
        cur.execute("ATTACH DATABASE ? AS tap_schema", (self.tap_schema_db,))
        result = cur.execute("""
            SELECT column_name, ucd, unit, datatype, description 
            FROM tap_schema.columns 
            WHERE table_name = ?
        """, (table_name,))
        rows = result.fetchall()
        conn.close()
        return rows
```

## Common Use Cases

### Importing Multiple Catalogs

```bash
# Import multiple catalogs from the same server
./import_tap_schema.py --url https://irsa.ipac.caltech.edu/TAP --catalog gaiadr3
./import_tap_schema.py --url https://irsa.ipac.caltech.edu/TAP --catalog wise_allsky
./import_tap_schema.py --url https://irsa.ipac.caltech.edu/TAP --catalog twomass
```

### Using a Custom Database Location

```bash
# Specify custom database path
./import_tap_schema.py --url https://irsa.ipac.caltech.edu/TAP --catalog gaiadr3 --db-path /var/lib/tap/tap_schema.db
```

### Updating Schema Metadata

The import tool uses `INSERT OR REPLACE`, so you can re-run it to update metadata:

```bash
# Re-import to update metadata
./import_tap_schema.py --url https://irsa.ipac.caltech.edu/TAP --catalog gaiadr3
```

### Verbose Logging

```bash
# Enable verbose logging for troubleshooting
./import_tap_schema.py --url https://irsa.ipac.caltech.edu/TAP --catalog gaiadr3 --verbose
```

## Testing

Run the test suite to verify functionality:

```bash
# Run unit tests
python3 test_import_tap_schema.py

# Run example demonstration
python3 example_tap_import.py
```

## Troubleshooting

### Connection Errors

If you see connection errors:
- Verify the TAP service URL is correct
- Check your network connection
- Ensure the TAP service is accessible from your location
- Some TAP services may have rate limiting

### No Tables Found

If the import reports "No tables found":
- Verify the schema/catalog name is correct
- Check if the schema exists: visit the TAP service in a browser
- Try querying the TAP service directly to list available schemas

### Database Locked

If you see "database is locked" errors:
- Close any open SQLite connections to the database
- Ensure no other processes are using the database file

## References

- [IVOA TAP 1.1 Specification](https://www.ivoa.net/documents/TAP/20181024/PR-TAP-1.1-20181024.html)
- [PyVO Documentation](https://pyvo.readthedocs.io/)
- [SQLite ATTACH Documentation](https://www.sqlite.org/lang_attach.html)
- Issue #45: TAP Schema Storage Implementation
- Issue #48: TAP Schema Import Tool (this implementation)

## Support

For issues or questions:
- Check the main README.md in this directory
- Review the script's built-in help: `./import_tap_schema.py --help`
- See example usage: `python3 example_tap_import.py`
