"""
TAP Schema Initialization Script

This script creates and populates the TAP_SCHEMA SQLite database with metadata
for the TAP service. It initializes the database schema and loads sample metadata
for demonstration purposes.

The populated database includes:
- TAP_SCHEMA schema with metadata tables
- public schema with sample astronomical catalogs (ztf_dr14, gaia_dr3)

Usage:
    python tap_schema_init.py [--db-path /path/to/database.db]
    
The database can be inspected using standard SQLite tools:
    sqlite3 tap_schema.db
    .tables
    SELECT * FROM schemas;
    SELECT * FROM tables;
"""

import sys
import argparse
from tap_schema_db import TAPSchemaDatabase


def populate_sample_data(db: TAPSchemaDatabase):
    """
    Populate the database with sample TAP_SCHEMA metadata.
    
    This function loads the same metadata that was previously stored in the
    TAP_SCHEMA_DATA dictionary, now persisted to SQLite.
    
    Args:
        db: TAPSchemaDatabase instance
    """
    print("Populating schemas...")
    
    # Insert schemas
    db.insert_schema(
        schema_name='TAP_SCHEMA',
        description='Schema describing the TAP service metadata',
        utype=None
    )
    
    db.insert_schema(
        schema_name='public',
        description='Default schema for astronomical catalogs',
        utype=None
    )
    
    print("Populating tables...")
    
    # Insert TAP_SCHEMA tables
    tap_schema_tables = [
        ('schemas', 'List of schemas in this TAP service'),
        ('tables', 'List of tables in this TAP service'),
        ('columns', 'List of columns in all tables'),
        ('keys', 'List of foreign keys'),
        ('key_columns', 'List of columns that participate in foreign keys'),
    ]
    
    for table_name, description in tap_schema_tables:
        db.insert_table(
            schema_name='TAP_SCHEMA',
            table_name=table_name,
            table_type='table',
            description=description,
            utype=None
        )
    
    # Insert public schema tables
    db.insert_table(
        schema_name='public',
        table_name='ztf_dr14',
        table_type='table',
        description='ZTF Data Release 14',
        utype=None
    )
    
    db.insert_table(
        schema_name='public',
        table_name='gaia_dr3',
        table_type='table',
        description='Gaia Data Release 3',
        utype=None
    )
    
    print("Populating columns...")
    
    # Insert columns for TAP_SCHEMA.schemas
    schema_columns = [
        ('schema_name', 'char', 256, 'Schema name', 1, 0, 1),
        ('description', 'char', 1024, 'Schema description', 0, 0, 1),
        ('utype', 'char', 512, 'UType for schema', 0, 0, 1),
    ]
    
    for col_name, datatype, size, description, principal, indexed, std in schema_columns:
        db.insert_column(
            table_name='TAP_SCHEMA.schemas',
            column_name=col_name,
            description=description,
            datatype=datatype,
            size=size,
            principal=principal,
            indexed=indexed,
            std=std,
            unit=None,
            ucd=None,
            utype=None
        )
    
    # Insert columns for TAP_SCHEMA.tables
    table_columns = [
        ('schema_name', 'char', 256, 'Schema name', 1, 1, 1),
        ('table_name', 'char', 256, 'Table name', 1, 1, 1),
        ('table_type', 'char', 16, 'Table type (table or view)', 1, 0, 1),
        ('description', 'char', 1024, 'Table description', 0, 0, 1),
        ('utype', 'char', 512, 'UType for table', 0, 0, 1),
    ]
    
    for col_name, datatype, size, description, principal, indexed, std in table_columns:
        db.insert_column(
            table_name='TAP_SCHEMA.tables',
            column_name=col_name,
            description=description,
            datatype=datatype,
            size=size,
            principal=principal,
            indexed=indexed,
            std=std,
            unit=None,
            ucd=None,
            utype=None
        )
    
    # Insert columns for TAP_SCHEMA.columns
    column_columns = [
        ('table_name', 'char', 256, 'Fully qualified table name', 1, 1, 1),
        ('column_name', 'char', 256, 'Column name', 1, 0, 1),
        ('description', 'char', 1024, 'Column description', 0, 0, 1),
        ('unit', 'char', 64, 'Unit of measure', 0, 0, 1),
        ('ucd', 'char', 64, 'UCD of column', 0, 0, 1),
        ('utype', 'char', 512, 'UType for column', 0, 0, 1),
        ('datatype', 'char', 64, 'ADQL datatype', 1, 0, 1),
        ('size', 'int', None, 'Size for variable-length types', 0, 0, 1),
        ('principal', 'int', None, '1 if column is principal', 0, 0, 1),
        ('indexed', 'int', None, '1 if column is indexed', 0, 0, 1),
        ('std', 'int', None, '1 if column is defined by a standard', 0, 0, 1),
    ]
    
    for col_name, datatype, size, description, principal, indexed, std in column_columns:
        db.insert_column(
            table_name='TAP_SCHEMA.columns',
            column_name=col_name,
            description=description,
            datatype=datatype,
            size=size,
            principal=principal,
            indexed=indexed,
            std=std,
            unit=None,
            ucd=None,
            utype=None
        )
    
    # Insert columns for TAP_SCHEMA.keys
    key_columns = [
        ('key_id', 'char', 256, 'Unique key identifier', 1, 1, 1),
        ('from_table', 'char', 256, 'Fully qualified table name', 1, 1, 1),
        ('target_table', 'char', 256, 'Target table name', 1, 1, 1),
        ('description', 'char', 1024, 'Key description', 0, 0, 1),
        ('utype', 'char', 512, 'UType for key', 0, 0, 1),
    ]
    
    for col_name, datatype, size, description, principal, indexed, std in key_columns:
        db.insert_column(
            table_name='TAP_SCHEMA.keys',
            column_name=col_name,
            description=description,
            datatype=datatype,
            size=size,
            principal=principal,
            indexed=indexed,
            std=std,
            unit=None,
            ucd=None,
            utype=None
        )
    
    # Insert columns for TAP_SCHEMA.key_columns
    key_column_columns = [
        ('key_id', 'char', 256, 'Key identifier', 1, 1, 1),
        ('from_column', 'char', 256, 'Column in from_table', 1, 0, 1),
        ('target_column', 'char', 256, 'Column in target_table', 1, 0, 1),
    ]
    
    for col_name, datatype, size, description, principal, indexed, std in key_column_columns:
        db.insert_column(
            table_name='TAP_SCHEMA.key_columns',
            column_name=col_name,
            description=description,
            datatype=datatype,
            size=size,
            principal=principal,
            indexed=indexed,
            std=std,
            unit=None,
            ucd=None,
            utype=None
        )
    
    # Insert columns for public.ztf_dr14
    ztf_columns = [
        ('objectid', 'long', None, 'Unique object identifier', 'meta.id;meta.main', 1, 1, 0),
        ('ra', 'double', None, 'Right Ascension', 'pos.eq.ra;meta.main', 1, 1, 0),
        ('dec', 'double', None, 'Declination', 'pos.eq.dec;meta.main', 1, 1, 0),
        ('mag', 'double', None, 'Mean magnitude', 'phot.mag', 1, 0, 0),
    ]
    
    for col_name, datatype, size, description, ucd, principal, indexed, std in ztf_columns:
        unit = 'deg' if col_name in ['ra', 'dec'] else ('mag' if col_name == 'mag' else None)
        db.insert_column(
            table_name='public.ztf_dr14',
            column_name=col_name,
            description=description,
            datatype=datatype,
            size=size,
            principal=principal,
            indexed=indexed,
            std=std,
            unit=unit,
            ucd=ucd,
            utype=None
        )
    
    # Insert columns for public.gaia_dr3
    gaia_columns = [
        ('source_id', 'long', None, 'Unique source identifier', 'meta.id;meta.main', 1, 1, 0),
        ('ra', 'double', None, 'Right Ascension', 'pos.eq.ra;meta.main', 1, 1, 0),
        ('dec', 'double', None, 'Declination', 'pos.eq.dec;meta.main', 1, 1, 0),
        ('phot_g_mean_mag', 'double', None, 'G-band mean magnitude', 'phot.mag;em.opt', 1, 0, 0),
    ]
    
    for col_name, datatype, size, description, ucd, principal, indexed, std in gaia_columns:
        unit = 'deg' if col_name in ['ra', 'dec'] else ('mag' if 'mag' in col_name else None)
        db.insert_column(
            table_name='public.gaia_dr3',
            column_name=col_name,
            description=description,
            datatype=datatype,
            size=size,
            principal=principal,
            indexed=indexed,
            std=std,
            unit=unit,
            ucd=ucd,
            utype=None
        )
    
    print("Database initialization complete!")


def main():
    """Main entry point for the initialization script."""
    parser = argparse.ArgumentParser(
        description='Initialize TAP_SCHEMA SQLite database'
    )
    parser.add_argument(
        '--db-path',
        default='tap_schema.db',
        help='Path to the SQLite database file (default: tap_schema.db)'
    )
    parser.add_argument(
        '--clear',
        action='store_true',
        help='Clear existing data before populating'
    )
    
    args = parser.parse_args()
    
    print(f"Initializing TAP_SCHEMA database at: {args.db_path}")
    
    with TAPSchemaDatabase(args.db_path) as db:
        # Create schema
        print("Creating database schema...")
        db.initialize_schema()
        
        # Clear existing data if requested
        if args.clear:
            print("Clearing existing data...")
            db.clear_all_tables()
        
        # Populate with sample data
        populate_sample_data(db)
        
        # Print summary
        print("\nDatabase summary:")
        print(f"  Schemas: {db.get_table_count('schemas')}")
        print(f"  Tables: {db.get_table_count('tables')}")
        print(f"  Columns: {db.get_table_count('columns')}")
        print(f"  Keys: {db.get_table_count('keys')}")
        print(f"  Key Columns: {db.get_table_count('key_columns')}")
        
        print(f"\nDatabase created successfully at: {args.db_path}")
        print("\nYou can inspect the database with:")
        print(f"  sqlite3 {args.db_path}")
        print("  .tables")
        print("  SELECT * FROM schemas;")


if __name__ == '__main__':
    main()
