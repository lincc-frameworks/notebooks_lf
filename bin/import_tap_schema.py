#!/usr/bin/env python3

"""
Import TAP schema metadata from an external TAP server into a local SQLite database.

This script queries a remote TAP server for schema and column metadata of specified
catalogs and inserts the metadata into a local tap_schema.db SQLite database.

Usage:
    python import_tap_schema.py --url <TAP_URL> --catalog <CATALOG_NAME> [options]

Examples:
    # Import Gaia DR3 schema from IRSA
    python import_tap_schema.py --url https://irsa.ipac.caltech.edu/TAP --catalog gaiadr3

    # Import with custom database path
    python import_tap_schema.py --url https://gea.esac.esa.int/tap-server/tap --catalog gaiadr3 --db-path /path/to/tap_schema.db

    # Import all tables from a schema
    python import_tap_schema.py --url https://irsa.ipac.caltech.edu/TAP --schema gaiadr3
"""

import argparse
import logging
import os
import sqlite3
import sys
from typing import Optional, List, Dict, Any

try:
    import pyvo
except ImportError:
    print("Error: pyvo library is required. Install it with: pip install pyvo", file=sys.stderr)
    sys.exit(1)


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TAPSchemaImporter:
    """Import TAP schema metadata from an external TAP server into a local SQLite database."""

    def __init__(self, db_path: str = "tap_schema.db"):
        """
        Initialize the TAP schema importer.

        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = os.path.abspath(db_path)
        self._init_database()

    def _init_database(self):
        """Initialize the SQLite database with TAP schema tables."""
        logger.info(f"Initializing database at {self.db_path}")

        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        # Create schemas table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS schemas (
            schema_name TEXT PRIMARY KEY,
            schema_description TEXT
        );
        """)

        # Create tables table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS tables (
            table_name TEXT PRIMARY KEY,
            schema_name TEXT,
            table_description TEXT,
            FOREIGN KEY(schema_name) REFERENCES schemas(schema_name)
        );
        """)

        # Create columns table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS columns (
            table_name TEXT,
            column_name TEXT,
            ucd TEXT,
            unit TEXT,
            datatype TEXT,
            description TEXT,
            PRIMARY KEY (table_name, column_name),
            FOREIGN KEY (table_name) REFERENCES tables(table_name)
        );
        """)

        conn.commit()
        conn.close()
        logger.info("Database initialized successfully")

    def query_tap_server(self, tap_url: str, schema_name: Optional[str] = None,
                        table_name: Optional[str] = None) -> tuple:
        """
        Query a TAP server for schema metadata.

        Args:
            tap_url: URL of the TAP service
            schema_name: Optional schema name to filter by
            table_name: Optional specific table name to query

        Returns:
            Tuple of (schemas_data, tables_data, columns_data)
        """
        logger.info(f"Connecting to TAP server: {tap_url}")

        try:
            service = pyvo.dal.TAPService(tap_url)
        except Exception as e:
            logger.error(f"Failed to connect to TAP service: {e}")
            raise

        # Query schemas
        schemas_data = self._query_schemas(service, schema_name)

        # Query tables
        tables_data = self._query_tables(service, schema_name, table_name)

        # Query columns
        columns_data = self._query_columns(service, schema_name, table_name)

        return schemas_data, tables_data, columns_data

    def _query_schemas(self, service, schema_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Query tap_schema.schemas table."""
        query = "SELECT schema_name, description FROM tap_schema.schemas"
        if schema_name:
            query += f" WHERE schema_name = '{schema_name}'"

        logger.info(f"Querying schemas: {query}")

        try:
            result = service.search(query)
            schemas = []
            for row in result:
                schemas.append({
                    'schema_name': str(row['schema_name']),
                    'description': str(row['description']) if row['description'] else None
                })
            logger.info(f"Found {len(schemas)} schema(s)")
            return schemas
        except Exception as e:
            logger.error(f"Failed to query schemas: {e}")
            raise

    def _query_tables(self, service, schema_name: Optional[str] = None,
                     table_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Query tap_schema.tables table."""
        query = "SELECT table_name, schema_name, description FROM tap_schema.tables"
        conditions = []

        if schema_name:
            conditions.append(f"schema_name = '{schema_name}'")
        if table_name:
            conditions.append(f"table_name = '{table_name}'")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        logger.info(f"Querying tables: {query}")

        try:
            result = service.search(query)
            tables = []
            for row in result:
                tables.append({
                    'table_name': str(row['table_name']),
                    'schema_name': str(row['schema_name']),
                    'description': str(row['description']) if row['description'] else None
                })
            logger.info(f"Found {len(tables)} table(s)")
            return tables
        except Exception as e:
            logger.error(f"Failed to query tables: {e}")
            raise

    def _query_columns(self, service, schema_name: Optional[str] = None,
                      table_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Query tap_schema.columns table."""
        query = """
        SELECT table_name, column_name, ucd, unit, datatype, description
        FROM tap_schema.columns
        """
        conditions = []

        if table_name:
            conditions.append(f"table_name = '{table_name}'")
        elif schema_name:
            # If only schema is specified, get columns for all tables in that schema
            query = """
            SELECT c.table_name, c.column_name, c.ucd, c.unit, c.datatype, c.description
            FROM tap_schema.columns c
            JOIN tap_schema.tables t ON c.table_name = t.table_name
            """
            conditions.append(f"t.schema_name = '{schema_name}'")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        logger.info(f"Querying columns")

        try:
            result = service.search(query)
            columns = []
            for row in result:
                columns.append({
                    'table_name': str(row['table_name']),
                    'column_name': str(row['column_name']),
                    'ucd': str(row['ucd']) if row['ucd'] else None,
                    'unit': str(row['unit']) if row['unit'] else None,
                    'datatype': str(row['datatype']) if row['datatype'] else None,
                    'description': str(row['description']) if row['description'] else None
                })
            logger.info(f"Found {len(columns)} column(s)")
            return columns
        except Exception as e:
            logger.error(f"Failed to query columns: {e}")
            raise

    def insert_metadata(self, schemas_data: List[Dict], tables_data: List[Dict],
                       columns_data: List[Dict]):
        """
        Insert metadata into the local SQLite database.

        Args:
            schemas_data: List of schema dictionaries
            tables_data: List of table dictionaries
            columns_data: List of column dictionaries
        """
        logger.info("Inserting metadata into database")

        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        try:
            # Insert schemas
            for schema in schemas_data:
                cur.execute("""
                INSERT OR REPLACE INTO schemas (schema_name, schema_description)
                VALUES (?, ?)
                """, (schema['schema_name'], schema['description']))
            logger.info(f"Inserted {len(schemas_data)} schema(s)")

            # Insert tables
            for table in tables_data:
                cur.execute("""
                INSERT OR REPLACE INTO tables (table_name, schema_name, table_description)
                VALUES (?, ?, ?)
                """, (table['table_name'], table['schema_name'], table['description']))
            logger.info(f"Inserted {len(tables_data)} table(s)")

            # Insert columns
            for column in columns_data:
                cur.execute("""
                INSERT OR REPLACE INTO columns (table_name, column_name, ucd, unit, datatype, description)
                VALUES (?, ?, ?, ?, ?, ?)
                """, (column['table_name'], column['column_name'], column['ucd'],
                     column['unit'], column['datatype'], column['description']))
            logger.info(f"Inserted {len(columns_data)} column(s)")

            conn.commit()
            logger.info("Metadata inserted successfully")

        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to insert metadata: {e}")
            raise
        finally:
            conn.close()

    def import_from_tap(self, tap_url: str, schema_name: Optional[str] = None,
                       table_name: Optional[str] = None):
        """
        Import schema metadata from a TAP server.

        Args:
            tap_url: URL of the TAP service
            schema_name: Optional schema name to filter by
            table_name: Optional specific table name to import
        """
        try:
            # Query the TAP server
            schemas_data, tables_data, columns_data = self.query_tap_server(
                tap_url, schema_name, table_name
            )

            if not tables_data:
                logger.warning("No tables found matching the criteria")
                return

            # Insert into local database
            self.insert_metadata(schemas_data, tables_data, columns_data)

            logger.info("Import completed successfully")

            # Print summary
            print("\nImport Summary:")
            print(f"  Schemas imported: {len(schemas_data)}")
            print(f"  Tables imported: {len(tables_data)}")
            print(f"  Columns imported: {len(columns_data)}")
            print(f"  Database: {self.db_path}")

        except Exception as e:
            logger.error(f"Import failed: {e}")
            raise


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Import TAP schema metadata from an external TAP server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Import Gaia DR3 schema from IRSA
  %(prog)s --url https://irsa.ipac.caltech.edu/TAP --catalog gaiadr3

  # Import specific table
  %(prog)s --url https://irsa.ipac.caltech.edu/TAP --table gaiadr3.gaia_source

  # Import all tables from a schema
  %(prog)s --url https://gea.esac.esa.int/tap-server/tap --schema gaiadr3

  # Use custom database path
  %(prog)s --url https://irsa.ipac.caltech.edu/TAP --catalog gaiadr3 --db-path /path/to/tap_schema.db
        """
    )

    parser.add_argument(
        "--url",
        required=True,
        help="URL of the TAP service (e.g., https://irsa.ipac.caltech.edu/TAP)"
    )

    parser.add_argument(
        "--catalog",
        help="Catalog/schema name to import (e.g., gaiadr3)"
    )

    parser.add_argument(
        "--schema",
        help="Schema name to import (alias for --catalog)"
    )

    parser.add_argument(
        "--table",
        help="Specific table name to import (e.g., gaiadr3.gaia_source)"
    )

    parser.add_argument(
        "--db-path",
        default="tap_schema.db",
        help="Path to the SQLite database file (default: tap_schema.db)"
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Determine schema and table names
    schema_name = args.catalog or args.schema
    table_name = args.table

    if not schema_name and not table_name:
        parser.error("Either --catalog/--schema or --table must be specified")

    # Parse table name if it includes schema (e.g., "gaiadr3.gaia_source")
    if table_name and '.' in table_name:
        parts = table_name.split('.', 1)
        if not schema_name:
            schema_name = parts[0]
        table_name = table_name  # Keep full table name for query

    try:
        # Create importer and run import
        importer = TAPSchemaImporter(db_path=args.db_path)
        importer.import_from_tap(args.url, schema_name=schema_name, table_name=table_name)

    except KeyboardInterrupt:
        print("\nImport cancelled by user", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        logger.error(f"Import failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
