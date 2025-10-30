#!/usr/bin/env python3

"""
Example demonstrating the TAP schema import functionality.

This example shows how the import_tap_schema.py script works by populating
a sample database with mock TAP schema data.
"""

import os
import sqlite3
import sys
import tempfile

# Add the bin directory to the path
sys.path.insert(0, os.path.dirname(__file__))

from import_tap_schema import TAPSchemaImporter


def main():
    """Demonstrate TAP schema import with sample data."""
    print("TAP Schema Import Demonstration")
    print("=" * 60)
    print()

    # Create a temporary database
    temp_db = tempfile.NamedTemporaryFile(mode='w', suffix='_tap_schema.db', delete=False)
    temp_db.close()
    db_path = temp_db.name

    try:
        # Initialize the importer
        print(f"Creating database at: {db_path}")
        importer = TAPSchemaImporter(db_path=db_path)
        print()

        # Sample metadata (similar to what would come from Gaia DR3)
        print("Populating with sample Gaia DR3-like metadata...")
        schemas_data = [
            {
                'schema_name': 'gaiadr3',
                'description': 'Gaia Data Release 3'
            }
        ]

        tables_data = [
            {
                'table_name': 'gaiadr3.gaia_source',
                'schema_name': 'gaiadr3',
                'description': 'Main source catalogue for Gaia DR3'
            },
            {
                'table_name': 'gaiadr3.gaia_source_lite',
                'schema_name': 'gaiadr3',
                'description': 'Lightweight version of the main source catalogue'
            }
        ]

        columns_data = [
            # gaia_source columns
            {
                'table_name': 'gaiadr3.gaia_source',
                'column_name': 'source_id',
                'ucd': 'meta.id;meta.main',
                'unit': None,
                'datatype': 'BIGINT',
                'description': 'Unique source identifier'
            },
            {
                'table_name': 'gaiadr3.gaia_source',
                'column_name': 'ra',
                'ucd': 'pos.eq.ra;meta.main',
                'unit': 'deg',
                'datatype': 'DOUBLE',
                'description': 'Right ascension'
            },
            {
                'table_name': 'gaiadr3.gaia_source',
                'column_name': 'dec',
                'ucd': 'pos.eq.dec;meta.main',
                'unit': 'deg',
                'datatype': 'DOUBLE',
                'description': 'Declination'
            },
            {
                'table_name': 'gaiadr3.gaia_source',
                'column_name': 'phot_g_mean_mag',
                'ucd': 'phot.mag;em.opt',
                'unit': 'mag',
                'datatype': 'REAL',
                'description': 'G-band mean magnitude'
            },
            {
                'table_name': 'gaiadr3.gaia_source',
                'column_name': 'parallax',
                'ucd': 'pos.parallax.trig',
                'unit': 'mas',
                'datatype': 'DOUBLE',
                'description': 'Parallax'
            },
            # gaia_source_lite columns
            {
                'table_name': 'gaiadr3.gaia_source_lite',
                'column_name': 'source_id',
                'ucd': 'meta.id;meta.main',
                'unit': None,
                'datatype': 'BIGINT',
                'description': 'Unique source identifier'
            },
            {
                'table_name': 'gaiadr3.gaia_source_lite',
                'column_name': 'ra',
                'ucd': 'pos.eq.ra;meta.main',
                'unit': 'deg',
                'datatype': 'DOUBLE',
                'description': 'Right ascension'
            },
            {
                'table_name': 'gaiadr3.gaia_source_lite',
                'column_name': 'dec',
                'ucd': 'pos.eq.dec;meta.main',
                'unit': 'deg',
                'datatype': 'DOUBLE',
                'description': 'Declination'
            },
        ]

        # Insert metadata
        importer.insert_metadata(schemas_data, tables_data, columns_data)
        print()

        # Query and display the imported data
        print("Import Summary:")
        print(f"  Schemas imported: {len(schemas_data)}")
        print(f"  Tables imported: {len(tables_data)}")
        print(f"  Columns imported: {len(columns_data)}")
        print()

        # Display database contents
        print("Database Contents:")
        print("-" * 60)
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        print("\n1. Schemas:")
        cur.execute("SELECT schema_name, schema_description FROM schemas")
        for row in cur.fetchall():
            print(f"   - {row[0]}: {row[1]}")

        print("\n2. Tables:")
        cur.execute("SELECT table_name, schema_name, table_description FROM tables")
        for row in cur.fetchall():
            print(f"   - {row[0]}")
            print(f"     Schema: {row[1]}")
            print(f"     Description: {row[2]}")

        print("\n3. Sample Columns (first 5):")
        cur.execute("""
            SELECT table_name, column_name, ucd, unit, datatype, description
            FROM columns
            LIMIT 5
        """)
        for row in cur.fetchall():
            print(f"   - {row[0]}.{row[1]}")
            print(f"     Type: {row[4]}, Unit: {row[3]}, UCD: {row[2]}")
            print(f"     Description: {row[5]}")

        # Demonstrate a JOIN query (as required by Issue #45)
        print("\n4. Schema-Tables JOIN Query:")
        print("   (Demonstrating the required JOIN from Issue #45)")
        cur.execute("""
            SELECT s.schema_name, s.schema_description, t.table_name
            FROM schemas s
            INNER JOIN tables t ON t.schema_name = s.schema_name
            ORDER BY s.schema_name, t.table_name
        """)
        for row in cur.fetchall():
            print(f"   - Schema: {row[0]}, Table: {row[2]}")

        conn.close()
        print()
        print("-" * 60)
        print(f"\nDatabase file: {db_path}")
        print("\nYou can inspect this database with:")
        print(f"  sqlite3 {db_path}")
        print(f'  sqlite3 {db_path} "SELECT * FROM tables;"')
        print()
        print("To use with TAP server, attach the database at runtime:")
        print('  conn = sqlite3.connect("main.db")')
        print(f'  conn.execute("ATTACH DATABASE \\"{db_path}\\" AS tap_schema")')
        print()

    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
