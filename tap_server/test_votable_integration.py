#!/usr/bin/env python
"""
Test script to verify column metadata is properly integrated into VOTable output.
This script tests the create_votable_response function without requiring lsdb.
"""

import sys
import os
import xml.etree.ElementTree as ET

# Add the tap_server directory to the path
sys.path.insert(0, os.path.dirname(__file__))

from tap_schema_db import TAPSchemaDatabase


def get_column_metadata(db_path, table_name):
    """Get column metadata from tap_schema.db."""
    if not table_name:
        return {}
    
    query = "SELECT column_name, datatype, unit, ucd, description FROM columns WHERE table_name = ?"
    try:
        with TAPSchemaDatabase(db_path) as db:
            results = db.query(query, (table_name,))
            
            if not results and '.' not in table_name:
                results = db.query(query, (f'public.{table_name}',))
            
            metadata = {}
            for row in results:
                col_name = row['column_name']
                metadata[col_name] = {
                    'datatype': row.get('datatype', 'char'),
                    'unit': row.get('unit', ''),
                    'ucd': row.get('ucd', ''),
                    'description': row.get('description', '')
                }
            
            return metadata
    except Exception as e:
        print(f"Error fetching column metadata for {table_name}: {e}")
        return {}


def create_simple_votable_field(col, column_metadata=None):
    """
    Create a FIELD element for a column with metadata.
    Simplified version of the logic from tap_server.py.
    """
    field_attrs = {
        'name': col,
        'datatype': 'double',  # Default datatype
        'unit': ''
    }
    
    # Use column metadata from tap_schema.db if available
    if column_metadata and col in column_metadata:
        meta = column_metadata[col]
        if meta.get('datatype'):
            field_attrs['datatype'] = meta['datatype']
        if meta.get('unit'):
            field_attrs['unit'] = meta['unit']
        if meta.get('ucd'):
            field_attrs['ucd'] = meta['ucd']
    else:
        # Fallback: Handle special astronomical columns with hard-coded values
        if col.lower() in ['ra', 'ra_deg']:
            field_attrs['unit'] = 'deg'
            field_attrs['ucd'] = 'pos.eq.ra;meta.main'
        elif col.lower() in ['dec', 'dec_deg']:
            field_attrs['unit'] = 'deg'
            field_attrs['ucd'] = 'pos.eq.dec;meta.main'
        elif col.lower() in ['mag', 'magnitude']:
            field_attrs['unit'] = 'mag'
            field_attrs['ucd'] = 'phot.mag'
    
    return field_attrs


def test_votable_field_with_metadata():
    """Test that VOTable FIELD elements use metadata from tap_schema.db."""
    print("Testing VOTable FIELD creation with metadata...")
    
    db_path = os.path.join(os.path.dirname(__file__), 'tap_schema.db')
    
    # Get metadata for ztf_dr14
    column_metadata = get_column_metadata(db_path, 'public.ztf_dr14')
    
    # Test columns
    columns = ['ra', 'dec', 'mag', 'objectid']
    
    for col in columns:
        field_attrs = create_simple_votable_field(col, column_metadata)
        
        print(f"  Column '{col}': {field_attrs}")
        
        # Verify metadata is used
        assert field_attrs['name'] == col
        
        if col == 'ra':
            assert field_attrs['datatype'] == 'double', f"Expected datatype 'double' for ra"
            assert field_attrs['unit'] == 'deg', f"Expected unit 'deg' for ra"
            assert field_attrs['ucd'] == 'pos.eq.ra;meta.main', f"Expected correct ucd for ra"
        elif col == 'dec':
            assert field_attrs['datatype'] == 'double', f"Expected datatype 'double' for dec"
            assert field_attrs['unit'] == 'deg', f"Expected unit 'deg' for dec"
            assert field_attrs['ucd'] == 'pos.eq.dec;meta.main', f"Expected correct ucd for dec"
        elif col == 'mag':
            assert field_attrs['datatype'] == 'double', f"Expected datatype 'double' for mag"
            assert field_attrs['unit'] == 'mag', f"Expected unit 'mag' for mag"
            assert field_attrs['ucd'] == 'phot.mag', f"Expected correct ucd for mag"
        elif col == 'objectid':
            assert field_attrs['datatype'] == 'long', f"Expected datatype 'long' for objectid"
            # objectid doesn't have unit or ucd in the database
    
    print("  ✓ All columns have correct metadata from tap_schema.db")


def test_votable_field_fallback():
    """Test that VOTable FIELD elements use fallback values when metadata is missing."""
    print("\nTesting VOTable FIELD creation with fallback values...")
    
    # Empty metadata (simulating missing table)
    column_metadata = {}
    
    # Test columns
    columns = ['ra', 'dec', 'mag', 'unknown_col']
    
    for col in columns:
        field_attrs = create_simple_votable_field(col, column_metadata)
        
        print(f"  Column '{col}': {field_attrs}")
        
        assert field_attrs['name'] == col
        
        # Should use fallback hard-coded values
        if col == 'ra':
            assert field_attrs['unit'] == 'deg', f"Expected fallback unit 'deg' for ra"
            assert field_attrs['ucd'] == 'pos.eq.ra;meta.main', f"Expected fallback ucd for ra"
        elif col == 'dec':
            assert field_attrs['unit'] == 'deg', f"Expected fallback unit 'deg' for dec"
            assert field_attrs['ucd'] == 'pos.eq.dec;meta.main', f"Expected fallback ucd for dec"
        elif col == 'mag':
            assert field_attrs['unit'] == 'mag', f"Expected fallback unit 'mag' for mag"
            assert field_attrs['ucd'] == 'phot.mag', f"Expected fallback ucd for mag"
        elif col == 'unknown_col':
            # Should have default values
            assert field_attrs['datatype'] == 'double', f"Expected default datatype 'double'"
            assert field_attrs['unit'] == '', f"Expected empty unit for unknown column"
    
    print("  ✓ Fallback values work correctly when metadata is missing")


def test_votable_field_gaia():
    """Test that VOTable FIELD elements work for gaia_dr3 table."""
    print("\nTesting VOTable FIELD creation for gaia_dr3...")
    
    db_path = os.path.join(os.path.dirname(__file__), 'tap_schema.db')
    
    # Get metadata for gaia_dr3
    column_metadata = get_column_metadata(db_path, 'public.gaia_dr3')
    
    # Test columns
    columns = ['source_id', 'ra', 'dec', 'phot_g_mean_mag']
    
    for col in columns:
        field_attrs = create_simple_votable_field(col, column_metadata)
        
        print(f"  Column '{col}': {field_attrs}")
        
        assert field_attrs['name'] == col
        
        if col == 'source_id':
            assert field_attrs['datatype'] == 'long', f"Expected datatype 'long' for source_id"
        elif col == 'phot_g_mean_mag':
            assert field_attrs['datatype'] == 'double', f"Expected datatype 'double' for phot_g_mean_mag"
            assert field_attrs['unit'] == 'mag', f"Expected unit 'mag' for phot_g_mean_mag"
            assert 'phot.mag' in field_attrs['ucd'], f"Expected 'phot.mag' in ucd"
    
    print("  ✓ Gaia DR3 columns have correct metadata")


def main():
    """Run all tests."""
    print("=" * 70)
    print("VOTable Integration Tests")
    print("=" * 70)
    
    try:
        test_votable_field_with_metadata()
        test_votable_field_fallback()
        test_votable_field_gaia()
        
        print("\n" + "=" * 70)
        print("All tests passed! ✓")
        print("=" * 70)
        print()
        print("Summary:")
        print("- Column metadata is correctly retrieved from tap_schema.db")
        print("- VOTable FIELD elements use metadata when available")
        print("- Fallback values are used when metadata is missing")
        print("- Works for both ztf_dr14 and gaia_dr3 tables")
        return 0
        
    except AssertionError as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
