#!/usr/bin/env python
"""
Simple test script to verify TAP server works with SQLite backend.
This test can be run without starting the server - it directly tests the functions.
"""

import sys
import os

# Add the tap_server directory to the path
sys.path.insert(0, os.path.dirname(__file__))

from tap_server import query_tap_schema, is_tap_schema_query


def test_is_tap_schema_query():
    """Test the is_tap_schema_query function."""
    print("Testing is_tap_schema_query()...")
    assert is_tap_schema_query('TAP_SCHEMA.schemas') == True
    assert is_tap_schema_query('TAP_SCHEMA.tables') == True
    assert is_tap_schema_query('tap_schema.columns') == True  # Case insensitive
    assert is_tap_schema_query('public.gaia_dr3') == False
    assert is_tap_schema_query('ztf_dr14') == False
    assert is_tap_schema_query(None) == False
    print("  ✓ All tests passed")


def test_query_schemas():
    """Test querying the schemas table."""
    print("\nTesting query_tap_schema('TAP_SCHEMA.schemas')...")
    data, columns = query_tap_schema('TAP_SCHEMA.schemas')
    
    assert len(data) == 2, f"Expected 2 schemas, got {len(data)}"
    assert 'schema_name' in columns, "Expected 'schema_name' in columns"
    assert 'description' in columns, "Expected 'description' in columns"
    
    schema_names = [row['schema_name'] for row in data]
    assert 'TAP_SCHEMA' in schema_names, "Expected 'TAP_SCHEMA' in results"
    assert 'public' in schema_names, "Expected 'public' in results"
    
    print(f"  ✓ Found {len(data)} schemas: {schema_names}")


def test_query_tables():
    """Test querying the tables table."""
    print("\nTesting query_tap_schema('TAP_SCHEMA.tables')...")
    data, columns = query_tap_schema('TAP_SCHEMA.tables')
    
    assert len(data) == 7, f"Expected 7 tables, got {len(data)}"
    assert 'schema_name' in columns, "Expected 'schema_name' in columns"
    assert 'table_name' in columns, "Expected 'table_name' in columns"
    
    print(f"  ✓ Found {len(data)} tables")


def test_query_with_filter():
    """Test querying with WHERE conditions."""
    print("\nTesting query with WHERE filter...")
    data, columns = query_tap_schema(
        'TAP_SCHEMA.tables',
        columns=['table_name', 'description'],
        conditions=[('schema_name', '==', 'public')]
    )
    
    assert len(data) == 2, f"Expected 2 tables in public schema, got {len(data)}"
    table_names = [row['table_name'] for row in data]
    assert 'ztf_dr14' in table_names, "Expected 'ztf_dr14' in results"
    assert 'gaia_dr3' in table_names, "Expected 'gaia_dr3' in results"
    
    print(f"  ✓ Found {len(data)} tables in public schema: {table_names}")


def test_query_with_limit():
    """Test querying with LIMIT."""
    print("\nTesting query with LIMIT...")
    data, columns = query_tap_schema(
        'TAP_SCHEMA.columns',
        limit=5
    )
    
    assert len(data) == 5, f"Expected 5 rows with LIMIT 5, got {len(data)}"
    print(f"  ✓ LIMIT clause works correctly")


def test_query_specific_columns():
    """Test querying specific columns."""
    print("\nTesting query with specific columns...")
    data, columns = query_tap_schema(
        'TAP_SCHEMA.schemas',
        columns=['schema_name']
    )
    
    assert len(columns) == 1, f"Expected 1 column, got {len(columns)}"
    assert columns[0] == 'schema_name', f"Expected 'schema_name', got {columns[0]}"
    assert len(data) == 2, f"Expected 2 rows, got {len(data)}"
    
    print(f"  ✓ Column filtering works correctly")


def main():
    """Run all tests."""
    print("=" * 70)
    print("TAP Server SQLite Backend Tests")
    print("=" * 70)
    
    try:
        test_is_tap_schema_query()
        test_query_schemas()
        test_query_tables()
        test_query_with_filter()
        test_query_with_limit()
        test_query_specific_columns()
        
        print("\n" + "=" * 70)
        print("All tests passed! ✓")
        print("=" * 70)
        return 0
        
    except AssertionError as e:
        print(f"\n✗ Test failed: {e}")
        return 1
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
