"""
Example Script for TAP Schema JOIN Queries

This script demonstrates how to query the TAP_SCHEMA database using SQL,
including the required JOIN between schemas and tables.

Usage:
    python example_join_query.py [--db-path /path/to/database.db]
"""

import argparse
from tap_schema_db import TAPSchemaDatabase


def example_join_query(db: TAPSchemaDatabase):
    """
    Demonstrate the JOIN query between schemas and tables.
    
    This is the key query mentioned in the issue:
    SELECT * FROM schemas INNER JOIN tables 
    ON tables.schema_name = schemas.schema_name
    """
    print("=" * 80)
    print("Example 1: JOIN schemas and tables")
    print("=" * 80)
    
    sql = """
        SELECT * 
        FROM schemas 
        INNER JOIN tables 
        ON tables.schema_name = schemas.schema_name
    """
    
    print(f"Query:\n{sql}\n")
    
    results = db.query(sql)
    
    print(f"Results ({len(results)} rows):\n")
    
    if results:
        # Print header
        keys = list(results[0].keys())
        header = " | ".join(f"{key:20}" for key in keys)
        print(header)
        print("-" * len(header))
        
        # Print rows
        for row in results:
            values = " | ".join(f"{str(row[key])[:20]:20}" for key in keys)
            print(values)
    else:
        print("No results found.")
    
    print()


def example_filtered_join(db: TAPSchemaDatabase):
    """
    Demonstrate a filtered JOIN query.
    
    This shows how to join schemas and tables with a WHERE clause.
    """
    print("=" * 80)
    print("Example 2: JOIN with WHERE clause - only public schema")
    print("=" * 80)
    
    sql = """
        SELECT 
            schemas.schema_name,
            schemas.description AS schema_description,
            tables.table_name,
            tables.description AS table_description
        FROM schemas 
        INNER JOIN tables 
        ON tables.schema_name = schemas.schema_name
        WHERE schemas.schema_name = 'public'
    """
    
    print(f"Query:\n{sql}\n")
    
    results = db.query(sql)
    
    print(f"Results ({len(results)} rows):\n")
    
    if results:
        for row in results:
            print(f"Schema: {row['schema_name']}")
            print(f"  Description: {row['schema_description']}")
            print(f"  Table: {row['table_name']}")
            print(f"  Table Description: {row['table_description']}")
            print()
    else:
        print("No results found.")


def example_simple_queries(db: TAPSchemaDatabase):
    """Demonstrate simple queries without JOINs."""
    print("=" * 80)
    print("Example 3: Simple queries")
    print("=" * 80)
    
    # Query all schemas
    print("\nAll schemas:")
    print("-" * 40)
    sql = "SELECT schema_name, description FROM schemas"
    results = db.query(sql)
    for row in results:
        print(f"  {row['schema_name']}: {row['description']}")
    
    # Query tables in public schema
    print("\nTables in 'public' schema:")
    print("-" * 40)
    sql = "SELECT table_name, description FROM tables WHERE schema_name = ?"
    results = db.query(sql, ('public',))
    for row in results:
        print(f"  {row['table_name']}: {row['description']}")
    
    # Count columns per table
    print("\nColumn count per table:")
    print("-" * 40)
    sql = "SELECT table_name, COUNT(*) as column_count FROM columns GROUP BY table_name"
    results = db.query(sql)
    for row in results:
        print(f"  {row['table_name']}: {row['column_count']} columns")
    
    print()


def example_advanced_join(db: TAPSchemaDatabase):
    """
    Demonstrate a more advanced JOIN query.
    
    This shows joining schemas, tables, and columns to get complete metadata.
    """
    print("=" * 80)
    print("Example 4: Three-way JOIN - schemas, tables, and columns")
    print("=" * 80)
    
    sql = """
        SELECT 
            s.schema_name,
            t.table_name,
            c.column_name,
            c.datatype,
            c.unit,
            c.description
        FROM schemas s
        INNER JOIN tables t ON t.schema_name = s.schema_name
        INNER JOIN columns c ON c.table_name = s.schema_name || '.' || t.table_name
        WHERE s.schema_name = 'public'
        ORDER BY t.table_name, c.column_name
        LIMIT 10
    """
    
    print(f"Query:\n{sql}\n")
    
    results = db.query(sql)
    
    print(f"Results (first {len(results)} rows):\n")
    
    current_table = None
    for row in results:
        table_full = f"{row['schema_name']}.{row['table_name']}"
        if table_full != current_table:
            current_table = table_full
            print(f"\nTable: {current_table}")
            print("  Columns:")
        
        unit_str = f" ({row['unit']})" if row['unit'] else ""
        print(f"    - {row['column_name']}: {row['datatype']}{unit_str}")
        if row['description']:
            print(f"      {row['description']}")
    
    print()


def main():
    """Main entry point for the example script."""
    parser = argparse.ArgumentParser(
        description='Example TAP Schema JOIN queries'
    )
    parser.add_argument(
        '--db-path',
        default='tap_schema.db',
        help='Path to the SQLite database file (default: tap_schema.db)'
    )
    
    args = parser.parse_args()
    
    print(f"Using TAP_SCHEMA database at: {args.db_path}\n")
    
    try:
        with TAPSchemaDatabase(args.db_path) as db:
            # Run example queries
            example_join_query(db)
            example_filtered_join(db)
            example_simple_queries(db)
            example_advanced_join(db)
            
        print("=" * 80)
        print("All examples completed successfully!")
        print("=" * 80)
        
    except Exception as e:
        print(f"Error: {e}")
        print("\nMake sure to initialize the database first:")
        print(f"  python tap_schema_init.py --db-path {args.db_path}")
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())
