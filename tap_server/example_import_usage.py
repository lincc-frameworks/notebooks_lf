#!/usr/bin/env python3
"""
Example: Using the TAP Schema Import Tool

This script demonstrates how to use the import_tap_schema module both
as a command-line tool and programmatically.
"""

import sys
import os

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(__file__))

from import_tap_schema import TAPSchemaImporter
from tap_schema_db import TAPSchemaDatabase


def example_command_line_usage():
    """
    Example command-line usage of the import tool.
    """
    print("=" * 70)
    print("Example: Command-Line Usage")
    print("=" * 70)
    print()
    print("# Import Gaia DR3 schema from ESA Gaia TAP server")
    print("python import_tap_schema.py --url https://gea.esac.esa.int/tap-server/tap --schema gaiadr3")
    print()
    print("# Import from IRSA Caltech TAP service")
    print("python import_tap_schema.py --url https://irsa.ipac.caltech.edu/TAP --schema wise_allwise")
    print()
    print("# Import to custom database location")
    print("python import_tap_schema.py --url <TAP_URL> --schema <schema> --db-path /path/to/my_db.sqlite")
    print()
    print("# Enable verbose logging")
    print("python import_tap_schema.py --url <TAP_URL> --schema <schema> --verbose")
    print()
    print("# Skip foreign key import (faster)")
    print("python import_tap_schema.py --url <TAP_URL> --schema <schema> --no-keys")
    print()


def example_programmatic_usage():
    """
    Example programmatic usage of the import tool.
    """
    print("=" * 70)
    print("Example: Programmatic Usage")
    print("=" * 70)
    print()
    print("```python")
    print("from import_tap_schema import TAPSchemaImporter")
    print("from tap_schema_db import TAPSchemaDatabase")
    print()
    print("# Use the importer as a context manager")
    print("tap_url = 'https://gea.esac.esa.int/tap-server/tap'")
    print("with TAPSchemaImporter(tap_url, 'my_catalog.db') as importer:")
    print("    # Import a complete schema")
    print("    importer.import_schema_metadata('gaiadr3', include_keys=True)")
    print()
    print("# Query the imported metadata")
    print("with TAPSchemaDatabase('my_catalog.db') as db:")
    print("    # Get all tables in the schema")
    print("    tables = db.query(\"SELECT * FROM tables WHERE schema_name = 'gaiadr3'\")")
    print("    for table in tables:")
    print("        print(f\"Table: {table['table_name']}\")")
    print()
    print("    # Get columns for a specific table")
    print("    columns = db.query(\"\"\"")
    print("        SELECT column_name, datatype, unit, description ")
    print("        FROM columns ")
    print("        WHERE table_name = 'gaiadr3.gaia_source'")
    print("    \"\"\")")
    print("    for col in columns:")
    print("        print(f\"  {col['column_name']}: {col['datatype']}\")")
    print("```")
    print()


def example_workflow():
    """
    Example complete workflow showing the import and query process.
    """
    print("=" * 70)
    print("Example: Complete Workflow")
    print("=" * 70)
    print()
    print("Step 1: Import catalog metadata from an external TAP server")
    print("-" * 70)
    print("$ python import_tap_schema.py --url https://irsa.ipac.caltech.edu/TAP --schema gaiadr3")
    print()
    print("Output:")
    print("  ======================================================================")
    print("  TAP Schema Import Tool")
    print("  ======================================================================")
    print("  TAP Server: https://irsa.ipac.caltech.edu/TAP")
    print("  Schema: gaiadr3")
    print("  Database: tap_schema.db")
    print("  ======================================================================")
    print("  2025-10-30 12:00:00 - INFO - Connecting to TAP service: https://irsa.ipac.caltech.edu/TAP")
    print("  2025-10-30 12:00:00 - INFO - ✓ Connected to TAP service")
    print("  2025-10-30 12:00:00 - INFO - Opening local database: tap_schema.db")
    print("  2025-10-30 12:00:00 - INFO - ✓ Local database ready")
    print("  2025-10-30 12:00:00 - INFO - Importing schema: gaiadr3")
    print("  2025-10-30 12:00:00 - INFO - Found schema: gaiadr3")
    print("  2025-10-30 12:00:00 - INFO - ✓ Imported schema metadata")
    print("  2025-10-30 12:00:00 - INFO - Importing tables for schema: gaiadr3")
    print("  2025-10-30 12:00:00 - INFO - Found 2 tables")
    print("  2025-10-30 12:00:00 - INFO -   ✓ Imported table: gaiadr3.gaia_source")
    print("  2025-10-30 12:00:00 - INFO -   ✓ Imported table: gaiadr3.gaia_source_lite")
    print("  2025-10-30 12:00:00 - INFO - Importing columns for 2 tables")
    print("  2025-10-30 12:00:00 - INFO -   Importing 256 columns for gaiadr3.gaia_source")
    print("  2025-10-30 12:00:00 - INFO -   Importing 45 columns for gaiadr3.gaia_source_lite")
    print("  2025-10-30 12:00:00 - INFO - ✓ Imported 301 total columns")
    print("  ======================================================================")
    print("  Import completed successfully!")
    print("    Schema: gaiadr3")
    print("    Tables: 2")
    print("  ======================================================================")
    print()
    print()
    print("Step 2: Inspect the imported metadata with SQLite")
    print("-" * 70)
    print("$ sqlite3 tap_schema.db")
    print()
    print("sqlite> SELECT * FROM tables WHERE schema_name = 'gaiadr3';")
    print("gaiadr3|gaia_source|table|Gaia DR3 main source catalog|")
    print("gaiadr3|gaia_source_lite|table|Gaia DR3 light source catalog|")
    print()
    print("sqlite> SELECT column_name, datatype, unit FROM columns")
    print("        WHERE table_name = 'gaiadr3.gaia_source' LIMIT 5;")
    print("source_id|long||")
    print("ra|double|deg")
    print("dec|double|deg")
    print("parallax|double|mas")
    print("pmra|double|mas/yr")
    print()
    print()
    print("Step 3: Use the metadata in your TAP server")
    print("-" * 70)
    print("$ python tap_server.py")
    print()
    print("The TAP server will now be able to respond to metadata queries:")
    print()
    print("$ curl -X POST http://localhost:5000/sync \\")
    print("  -d 'REQUEST=doQuery' \\")
    print("  -d 'LANG=ADQL' \\")
    print("  -d 'QUERY=SELECT * FROM TAP_SCHEMA.tables WHERE schema_name=\"gaiadr3\"'")
    print()


def main():
    """Main entry point."""
    print()
    example_command_line_usage()
    print()
    example_programmatic_usage()
    print()
    example_workflow()
    print()
    print("=" * 70)
    print("For more information, see the README.md file")
    print("=" * 70)


if __name__ == '__main__':
    main()
