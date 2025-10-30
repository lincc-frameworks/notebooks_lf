#!/usr/bin/env python3
"""
TAP Schema Import Tool

This tool queries an external TAP server for schema metadata and imports it
into the local TAP_SCHEMA SQLite database. It follows the IVOA TAP 1.1
specification for TAP_SCHEMA tables.

Usage:
    python import_tap_schema.py --url <TAP_URL> --schema <schema_name>
    
Example:
    python import_tap_schema.py --url https://irsa.ipac.caltech.edu/TAP --schema gaiadr3
    python import_tap_schema.py --url https://gea.esac.esa.int/tap-server/tap --schema gaiadr3

Reference:
    IVOA TAP 1.1 Specification: https://www.ivoa.net/documents/TAP/20181024/PR-TAP-1.1-20181024.html
"""

import argparse
import sys
import logging
import pyvo

from tap_schema_db import TAPSchemaDatabase


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TAPSchemaImporter:
    """
    Imports TAP_SCHEMA metadata from an external TAP server into local database.
    """
    
    def __init__(self, tap_url: str, db_path: str = 'tap_schema.db'):
        """
        Initialize the TAP schema importer.
        
        Args:
            tap_url: URL of the external TAP server
            db_path: Path to local SQLite database
        """
        self.tap_url = tap_url
        self.db_path = db_path
        self.service = None
        self.db = None
        
    @staticmethod
    def _escape_adql_string(value: str) -> str:
        """
        Escape a string value for safe use in ADQL queries.
        
        Args:
            value: String to escape
            
        Returns:
            Escaped string safe for ADQL query
        """
        # Escape single quotes by doubling them (SQL/ADQL standard)
        return value.replace("'", "''")
        
    def connect(self):
        """Connect to both the TAP service and local database."""
        logger.info(f"Connecting to TAP service: {self.tap_url}")
        try:
            self.service = pyvo.dal.TAPService(self.tap_url)
            logger.info("✓ Connected to TAP service")
        except Exception as e:
            logger.error(f"Failed to connect to TAP service: {e}")
            raise
            
        logger.info(f"Opening local database: {self.db_path}")
        self.db = TAPSchemaDatabase(self.db_path)
        self.db.connect()
        # Ensure schema exists
        self.db.initialize_schema()
        logger.info("✓ Local database ready")
        
    def close(self):
        """Close database connection."""
        if self.db:
            self.db.close()
            
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        
    def query_tap_schema_table(self, table_name: str, where_clause: str = None) -> list[dict[str, any]]:
        """
        Query a TAP_SCHEMA table from the external TAP server.
        
        Args:
            table_name: Name of the TAP_SCHEMA table (e.g., 'schemas', 'tables', 'columns')
            where_clause: Optional WHERE clause to filter results
            
        Returns:
            List of dictionaries containing the query results
        """
        # Validate table_name using whitelist of allowed TAP_SCHEMA tables
        allowed_tables = {'schemas', 'tables', 'columns', 'keys', 'key_columns'}
        if table_name not in allowed_tables:
            raise ValueError(f"Invalid table name: {table_name}. Must be one of {allowed_tables}")
            
        query = f"SELECT * FROM TAP_SCHEMA.{table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"
            
        logger.debug(f"Querying: {query}")
        
        try:
            result = self.service.search(query)
            # Convert to list of dictionaries
            data = []
            for row in result:
                row_dict = {}
                for col_name in result.fieldnames:
                    value = row[col_name]
                    # Handle masked values (NULLs)
                    try:
                        if hasattr(value, 'mask') and value.mask:
                            value = None
                    except (AttributeError, ValueError):
                        # If mask check fails, keep the original value
                        pass
                    row_dict[col_name] = value
                data.append(row_dict)
            return data
        except Exception as e:
            logger.error(f"Query failed: {e}")
            logger.debug(f"Query was: {query}")
            raise
            
    def import_schema(self, schema_name: str):
        """
        Import a specific schema from the external TAP server.
        
        Args:
            schema_name: Name of the schema to import
        """
        logger.info(f"Importing schema: {schema_name}")
        
        # Query for the schema
        escaped_schema = self._escape_adql_string(schema_name)
        schemas = self.query_tap_schema_table(
            'schemas', 
            f"schema_name = '{escaped_schema}'"
        )
        
        if not schemas:
            logger.warning(f"Schema '{schema_name}' not found on TAP server")
            # Try to import it anyway as some servers might not have TAP_SCHEMA.schemas
            logger.info(f"Attempting to import tables from schema '{schema_name}' anyway...")
        else:
            schema_data = schemas[0]
            logger.info(f"Found schema: {schema_data.get('schema_name')}")
            
            # Insert schema into local database
            self.db.insert_schema(
                schema_name=schema_data.get('schema_name'),
                description=schema_data.get('description'),
                utype=schema_data.get('utype')
            )
            logger.info(f"✓ Imported schema metadata")
        
    def import_tables(self, schema_name: str) -> list[str]:
        """
        Import tables for a schema from the external TAP server.
        
        Args:
            schema_name: Name of the schema whose tables to import
            
        Returns:
            List of fully qualified table names (schema.table)
        """
        logger.info(f"Importing tables for schema: {schema_name}")
        
        # Query for tables in the schema
        escaped_schema = self._escape_adql_string(schema_name)
        tables = self.query_tap_schema_table(
            'tables',
            f"schema_name = '{escaped_schema}'"
        )
        
        if not tables:
            logger.warning(f"No tables found for schema '{schema_name}'")
            return []
            
        logger.info(f"Found {len(tables)} tables")
        
        table_names = []
        for table_data in tables:
            schema = table_data.get('schema_name')
            table = table_data.get('table_name')
            
            # Insert table into local database
            self.db.insert_table(
                schema_name=schema,
                table_name=table,
                table_type=table_data.get('table_type', 'table'),
                description=table_data.get('description'),
                utype=table_data.get('utype')
            )
            
            # Build fully qualified name
            full_table_name = f"{schema}.{table}" if schema else table
            table_names.append(full_table_name)
            logger.info(f"  ✓ Imported table: {full_table_name}")
            
        return table_names
        
    def import_columns(self, table_names: list[str], local_table_name: str = None):
        """
        Import columns for specified tables from the external TAP server.
        
        Args:
            table_names: List of fully qualified table names
            local_table_name: Optional local name to use for storing columns.
                            Only used when importing a single table with a different name.
        """
        logger.info(f"Importing columns for {len(table_names)} tables")
        
        total_columns = 0
        for table_name in table_names:
            # Query for columns in this table
            escaped_table = self._escape_adql_string(table_name)
            columns = self.query_tap_schema_table(
                'columns',
                f"table_name = '{escaped_table}'"
            )
            
            if not columns:
                logger.warning(f"No columns found for table '{table_name}'")
                continue
                
            logger.info(f"  Importing {len(columns)} columns for {table_name}")
            
            for col_data in columns:
                # Map column metadata to database fields
                kwargs = {}
                for field in ['description', 'unit', 'ucd', 'utype', 'datatype', 
                             'size', 'principal', 'indexed', 'std']:
                    value = col_data.get(field)
                    if value is not None:
                        kwargs[field] = value
                
                # Use local_table_name if provided and importing a single table, otherwise use original table name        
                stored_table_name = local_table_name if local_table_name and len(table_names) == 1 else col_data.get('table_name')
                
                self.db.insert_column(
                    table_name=stored_table_name,
                    column_name=col_data.get('column_name'),
                    **kwargs
                )
                total_columns += 1
                
            logger.debug(f"    ✓ Imported {len(columns)} columns")
            
        logger.info(f"✓ Imported {total_columns} total columns")
        
    def import_keys(self, table_names: list[str]):
        """
        Import foreign keys for specified tables from the external TAP server.
        
        Args:
            table_names: List of fully qualified table names
        """
        logger.info("Importing foreign keys")
        
        try:
            # Query for all keys
            keys = self.query_tap_schema_table('keys')
            
            if not keys:
                logger.info("No foreign keys found")
                return
                
            # Filter keys related to our tables
            relevant_keys = [
                k for k in keys 
                if k.get('from_table') in table_names or k.get('target_table') in table_names
            ]
            
            if not relevant_keys:
                logger.info("No relevant foreign keys found for imported tables")
                return
                
            logger.info(f"Found {len(relevant_keys)} foreign keys")
            
            for key_data in relevant_keys:
                self.db.insert_key(
                    key_id=key_data.get('key_id'),
                    from_table=key_data.get('from_table'),
                    target_table=key_data.get('target_table'),
                    description=key_data.get('description'),
                    utype=key_data.get('utype')
                )
                
                # Import key columns
                key_id = key_data.get('key_id')
                if key_id is not None:
                    escaped_key_id = self._escape_adql_string(str(key_id))
                    key_columns = self.query_tap_schema_table(
                        'key_columns',
                        f"key_id = '{escaped_key_id}'"
                    )
                else:
                    key_columns = []
                
                for kc_data in key_columns:
                    self.db.insert_key_column(
                        key_id=kc_data.get('key_id'),
                        from_column=kc_data.get('from_column'),
                        target_column=kc_data.get('target_column')
                    )
                    
            logger.info(f"✓ Imported {len(relevant_keys)} foreign keys")
            
        except Exception as e:
            # Some TAP servers may not have keys/key_columns tables
            logger.warning(f"Could not import foreign keys: {e}")
            logger.info("Continuing without foreign key metadata...")
            
    def import_table_by_name(self, table_name: str, include_keys: bool = True, local_table_name: str = None):
        """
        Import metadata for a specific table by its name from external TAP server.
        
        This method queries the TAP_SCHEMA.tables to find the table and its schema,
        then imports all metadata for that table.
        
        Args:
            table_name: Name of the table to import (e.g., 'gaia_dr3_source')
            include_keys: Whether to import foreign key relationships
            local_table_name: Optional name to use when storing the table locally.
                            If not provided, the original table name is used.
            
        Returns:
            True if import successful, False otherwise
        """
        try:
            logger.info(f"Looking up table: {table_name}")
            
            # Query for the table to find its schema
            escaped_table = self._escape_adql_string(table_name)
            tables = self.query_tap_schema_table(
                'tables',
                f"table_name = '{escaped_table}'"
            )
            
            if not tables:
                logger.error(f"Table '{table_name}' not found on TAP server")
                return False
            
            table_data = tables[0]
            schema_name = table_data.get('schema_name')
            
            logger.info(f"Found table '{table_name}' in schema '{schema_name}'")
            
            # Import the schema first
            self.import_schema(schema_name)
            
            # Determine the name to use for storing the table locally
            stored_table_name = local_table_name if local_table_name else table_data.get('table_name')
            
            # Insert the table metadata
            self.db.insert_table(
                schema_name=schema_name,
                table_name=stored_table_name,
                table_type=table_data.get('table_type', 'table'),
                description=table_data.get('description'),
                utype=table_data.get('utype')
            )
            
            # Use table name as-is from the TAP server (not schema.table format)
            # Some TAP servers store table_name in columns without schema prefix
            if local_table_name:
                logger.info(f"✓ Imported table: {table_name} as '{stored_table_name}' (schema: {schema_name})")
            else:
                logger.info(f"✓ Imported table: {table_name} (schema: {schema_name})")
            
            # Import columns for this table using the table name as-is
            self.import_columns([table_name], local_table_name=stored_table_name)
            
            # Import foreign keys if requested
            if include_keys:
                self.import_keys([table_name])
            
            logger.info("=" * 70)
            logger.info("Import completed successfully!")
            logger.info(f"  Table: {table_name}")
            logger.info(f"  Schema: {schema_name}")
            logger.info("=" * 70)
            
            return True
            
        except Exception as e:
            logger.error(f"Import failed: {e}")
            raise
    
    def import_schema_metadata(self, schema_name: str, include_keys: bool = True):
        """
        Import complete metadata for a schema from external TAP server.
        
        Args:
            schema_name: Name of the schema to import
            include_keys: Whether to import foreign key relationships
        """
        try:
            # Import schema
            self.import_schema(schema_name)
            
            # Import tables
            table_names = self.import_tables(schema_name)
            
            if not table_names:
                logger.error(f"No tables found for schema '{schema_name}'. Import failed.")
                return False
                
            # Import columns
            self.import_columns(table_names)
            
            # Import foreign keys if requested
            if include_keys:
                self.import_keys(table_names)
                
            logger.info("=" * 70)
            logger.info("Import completed successfully!")
            logger.info(f"  Schema: {schema_name}")
            logger.info(f"  Tables: {len(table_names)}")
            logger.info("=" * 70)
            
            return True
            
        except Exception as e:
            logger.error(f"Import failed: {e}")
            raise


def main():
    """Main entry point for the CLI tool."""
    parser = argparse.ArgumentParser(
        description='Import TAP_SCHEMA metadata from an external TAP server',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Import entire Gaia DR3 schema from IRSA
  python import_tap_schema.py --url https://irsa.ipac.caltech.edu/TAP --schema gaiadr3
  
  # Import a specific table by name
  python import_tap_schema.py --url https://irsa.ipac.caltech.edu/TAP --table gaia_dr3_source
  
  # Import a table with a different local name
  python import_tap_schema.py --url https://irsa.ipac.caltech.edu/TAP --table gaia_dr3_source --local-table-name my_gaia_table
  
  # Import from Gaia TAP server
  python import_tap_schema.py --url https://gea.esac.esa.int/tap-server/tap --schema gaiadr3
  
  # Import to a custom database location
  python import_tap_schema.py --url <TAP_URL> --schema <schema> --db-path /path/to/db.sqlite
  
  # Skip foreign key import for faster processing
  python import_tap_schema.py --url <TAP_URL> --table <table> --no-keys
        """
    )
    
    parser.add_argument(
        '--url',
        required=True,
        help='URL of the external TAP server (e.g., https://irsa.ipac.caltech.edu/TAP)'
    )
    
    # Create mutually exclusive group for schema or table
    target_group = parser.add_mutually_exclusive_group(required=True)
    target_group.add_argument(
        '--schema',
        help='Name of the schema/catalog to import (e.g., gaiadr3, wise_allwise)'
    )
    target_group.add_argument(
        '--table',
        help='Name of a specific table to import (e.g., gaia_dr3_source)'
    )
    
    parser.add_argument(
        '--local-table-name',
        help='Optional local name for the imported table (only valid with --table). '
             'Use this to store the table with a different name in the local database.'
    )
    
    parser.add_argument(
        '--db-path',
        default='tap_schema.db',
        help='Path to the local SQLite database (default: tap_schema.db)'
    )
    
    parser.add_argument(
        '--no-keys',
        action='store_true',
        help='Skip importing foreign key relationships'
    )
    
    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Enable verbose debug logging'
    )
    
    args = parser.parse_args()
    
    # Validate that --local-table-name is only used with --table
    if args.local_table_name and not args.table:
        parser.error("--local-table-name can only be used with --table")
    
    # Set logging level
    if args.verbose:
        logger.setLevel(logging.DEBUG)
        
    # Display header
    print("=" * 70)
    print("TAP Schema Import Tool")
    print("=" * 70)
    print(f"TAP Server: {args.url}")
    if args.schema:
        print(f"Schema: {args.schema}")
    else:
        print(f"Table: {args.table}")
        if args.local_table_name:
            print(f"Local table name: {args.local_table_name}")
    print(f"Database: {args.db_path}")
    print("=" * 70)
    
    try:
        with TAPSchemaImporter(args.url, args.db_path) as importer:
            if args.schema:
                success = importer.import_schema_metadata(
                    args.schema,
                    include_keys=not args.no_keys
                )
                query_hint = f"SELECT * FROM tables WHERE schema_name = '{args.schema}';"
            else:
                success = importer.import_table_by_name(
                    args.table,
                    include_keys=not args.no_keys,
                    local_table_name=args.local_table_name
                )
                display_table_name = args.local_table_name if args.local_table_name else args.table
                query_hint = f"SELECT * FROM tables WHERE table_name = '{display_table_name}';"
            
            if success:
                print("\n✓ Import completed successfully!")
                print(f"\nYou can now query the metadata using:")
                print(f"  sqlite3 {args.db_path}")
                print(f"  {query_hint}")
                sys.exit(0)
            else:
                print("\n✗ Import failed!")
                sys.exit(1)
                
    except KeyboardInterrupt:
        print("\n\nImport cancelled by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Import failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
