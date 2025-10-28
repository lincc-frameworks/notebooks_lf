# TAP Schema SQLite Implementation

## Overview

This document describes the SQLite-based implementation of TAP_SCHEMA metadata storage for the TAP server prototype. This implementation satisfies the requirements specified in issue #45.

## Implementation Details

### Architecture

The TAP_SCHEMA storage layer consists of three main components:

1. **Database Module** (`tap_schema_db.py`): Core database operations
2. **Initialization Script** (`tap_schema_init.py`): Database creation and population
3. **Integration** (`tap_server.py`): TAP server integration

### Database Schema

The SQLite database implements five tables as defined in the TAP 1.1 specification. **Important:** All table names include the `tap_schema.` prefix (e.g., `"tap_schema.schemas"`) to comply with the TAP specification's fully-qualified table naming convention.

#### `tap_schema.schemas`
- `schema_name` (TEXT, PRIMARY KEY): Name of the schema
- `description` (TEXT): Schema description
- `utype` (TEXT): UType for the schema

#### `tap_schema.tables`
- `schema_name` (TEXT, FOREIGN KEY): Schema containing the table
- `table_name` (TEXT): Name of the table
- `table_type` (TEXT): Type of table ('table' or 'view')
- `description` (TEXT): Table description
- `utype` (TEXT): UType for the table
- PRIMARY KEY: (schema_name, table_name)

#### `tap_schema.columns`
- `table_name` (TEXT): Fully qualified table name (schema.table)
- `column_name` (TEXT): Name of the column
- `description` (TEXT): Column description
- `unit` (TEXT): Unit of measure
- `ucd` (TEXT): Unified Content Descriptor
- `utype` (TEXT): UType for the column
- `datatype` (TEXT): ADQL datatype
- `size` (INTEGER): Size for variable-length types
- `principal` (INTEGER): 1 if principal column, 0 otherwise
- `indexed` (INTEGER): 1 if indexed, 0 otherwise
- `std` (INTEGER): 1 if defined by a standard, 0 otherwise
- PRIMARY KEY: (table_name, column_name)

#### `tap_schema.keys`
- `key_id` (TEXT, PRIMARY KEY): Unique key identifier
- `from_table` (TEXT): Source table name
- `target_table` (TEXT): Target table name
- `description` (TEXT): Key description
- `utype` (TEXT): UType for the key

#### `tap_schema.key_columns`
- `key_id` (TEXT, FOREIGN KEY): Foreign key identifier
- `from_column` (TEXT): Column name in source table
- `target_column` (TEXT): Column name in target table
- PRIMARY KEY: (key_id, from_column)

### Key Features

#### 1. Native SQL Query Support

The implementation uses SQLite's native SQL engine, enabling:
- SELECT with column projection
- WHERE clauses with comparison operators
- JOIN operations (INNER, LEFT, RIGHT, CROSS)
- ORDER BY, GROUP BY, HAVING
- Aggregate functions (COUNT, SUM, AVG, etc.)
- Subqueries

Example JOIN query (as required in issue #45):
```sql
SELECT * 
FROM "tap_schema.schemas"
INNER JOIN "tap_schema.tables" 
ON "tap_schema.tables".schema_name = "tap_schema.schemas".schema_name
```

#### 2. Zero External Dependencies

Uses only Python's built-in `sqlite3` module. No external libraries required beyond Flask for the web server.

#### 3. Database Inspectability

The SQLite database file can be inspected using standard tools:

**Command Line (sqlite3):**
```bash
sqlite3 tap_schema.db
.tables
SELECT * FROM "tap_schema.schemas";
```

**GUI Tools:**
- DB Browser for SQLite
- SQLiteStudio
- DBeaver
- DataGrip

#### 4. Security

The implementation includes SQL injection protection:
- Parameterized queries for user input
- Table name validation against whitelist
- Column name sanitization (alphanumeric + underscore only)

#### 5. Extensibility

The database can be easily extended:
- Add new schemas and tables using insert methods
- Execute arbitrary SQL queries for complex operations
- Support for future TAP 1.1 features (e.g., additional metadata)

## Usage Examples

### Initialize Database

```bash
# Create new database with sample data
python tap_schema_init.py

# Recreate database (clear existing data)
python tap_schema_init.py --clear

# Use custom database path
python tap_schema_init.py --db-path /path/to/my_tap_schema.db
```

### Query Database Directly

```python
from tap_schema_db import TAPSchemaDatabase

# Connect to database
with TAPSchemaDatabase('tap_schema.db') as db:
    # Simple query
    results = db.query('SELECT * FROM "tap_schema.schemas"')
    for row in results:
        print(f"{row['schema_name']}: {row['description']}")
    
    # Parameterized query
    results = db.query(
        'SELECT * FROM "tap_schema.tables" WHERE schema_name = ?',
        ('public',)
    )
    
    # JOIN query
    results = db.query("""
        SELECT s.schema_name, s.description, t.table_name
        FROM "tap_schema.schemas" s
        INNER JOIN "tap_schema.tables" t ON t.schema_name = s.schema_name
    """)
```

### Add Metadata

```python
from tap_schema_db import TAPSchemaDatabase

with TAPSchemaDatabase('tap_schema.db') as db:
    # Add a new schema
    db.insert_schema(
        schema_name='myschema',
        description='My custom schema'
    )
    
    # Add a new table
    db.insert_table(
        schema_name='myschema',
        table_name='mytable',
        table_type='table',
        description='My custom table'
    )
    
    # Add columns
    db.insert_column(
        table_name='myschema.mytable',
        column_name='id',
        datatype='long',
        description='Unique identifier',
        principal=1,
        indexed=1
    )
```

### Run Example Queries

```bash
# Run all example JOIN queries
python example_join_query.py

# Use custom database
python example_join_query.py --db-path my_tap_schema.db
```

### Test Implementation

```bash
# Run test suite
python test_sqlite_backend.py
```

## Integration with TAP Server

The TAP server (`tap_server.py`) has been updated to use the SQLite backend:

1. Database is automatically loaded on server startup
2. TAP_SCHEMA queries are routed to `query_tap_schema()` function
3. `query_tap_schema()` translates to SQL and executes against SQLite
4. Results are converted to VOTable format for TAP protocol compliance

The integration is transparent - ADQL queries to TAP_SCHEMA tables work exactly as before, but now execute against SQLite instead of in-memory data structures.

## Performance Considerations

### Indexing

The database includes indexes on:
- `tables.schema_name` - For filtering by schema
- `tables.table_name` - For table lookups
- `columns.table_name` - For column queries
- `keys.from_table` and `keys.target_table` - For foreign key queries
- `key_columns.key_id` - For key column lookups

### Query Optimization

SQLite automatically optimizes queries using:
- Index selection
- Query planning
- Statistics-based optimization

For large metadata sets, consider:
- Using EXPLAIN QUERY PLAN to analyze queries
- Adding covering indexes for frequently accessed columns
- Using ANALYZE to update statistics

## Testing

The implementation has been tested with:

1. **Unit Tests** (`test_sqlite_backend.py`):
   - Query validation
   - Filter conditions
   - Column selection
   - Limit clauses

2. **Integration Tests**:
   - JOIN queries (schemas ⋈ tables)
   - Filtered JOINs with WHERE
   - Three-way JOINs (schemas ⋈ tables ⋈ columns)

3. **Manual Tests**:
   - SQLite CLI inspection
   - TAP server integration
   - ADQL query execution

All tests pass successfully.

## Migration Notes

For users upgrading from the in-memory implementation:

1. The database must be initialized before running the server:
   ```bash
   python tap_schema_init.py
   ```

2. The `tap_schema.db` file will be created in the `tap_server` directory

3. Custom metadata must be added using the database API instead of modifying the `TAP_SCHEMA_DATA` dictionary

4. Query behavior is identical - no changes to ADQL queries are required

## Future Enhancements

Potential improvements for future versions:

1. **Metadata Import**: Tools to import metadata from LSDB catalogs
2. **Metadata Cache**: Automatic updates when catalogs change
3. **Schema Versioning**: Track metadata changes over time
4. **Backup/Restore**: Utilities for database backup and migration
5. **Multi-Database**: Support for multiple TAP services in one database
6. **Performance Monitoring**: Query logging and performance analysis

## References

- [IVOA TAP 1.1 Specification](https://www.ivoa.net/documents/TAP/20181024/PR-TAP-1.1-20181024.html)
- [Python sqlite3 Documentation](https://docs.python.org/3/library/sqlite3.html)
- [SQLite Documentation](https://www.sqlite.org/docs.html)
- [Issue #45](https://github.com/lincc-frameworks/notebooks_lf/issues/45)

## License

This implementation is part of the LINCC Frameworks notebooks repository and follows the same license.
