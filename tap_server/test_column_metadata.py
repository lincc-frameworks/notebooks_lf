#!/usr/bin/env python
"""
Test script to verify column metadata retrieval from tap_schema.db
"""

import sys
import os

# Add the tap_server directory to the path
sys.path.insert(0, os.path.dirname(__file__))

from tap_server import get_column_metadata


def test_get_column_metadata_ztf():
    """Test fetching column metadata for ztf_dr14 table."""
    print("Testing get_column_metadata('ztf_dr14')...")
    
    # Try without schema prefix
    metadata = get_column_metadata('ztf_dr14')
    
    # If empty, try with public prefix
    if not metadata:
        print("  No results without schema prefix, trying 'public.ztf_dr14'...")
        metadata = get_column_metadata('public.ztf_dr14')
    
    assert metadata, "Expected metadata for ztf_dr14, got empty dict"
    
    # Check for expected columns
    expected_cols = ['ra', 'dec', 'mag', 'objectid']
    for col in expected_cols:
        assert col in metadata, f"Expected column '{col}' in metadata"
    
    # Check ra column has correct metadata
    ra_meta = metadata['ra']
    assert ra_meta['datatype'] == 'double', f"Expected ra datatype 'double', got '{ra_meta['datatype']}'"
    assert ra_meta['unit'] == 'deg', f"Expected ra unit 'deg', got '{ra_meta['unit']}'"
    assert 'pos.eq.ra' in ra_meta['ucd'], f"Expected ra ucd to contain 'pos.eq.ra', got '{ra_meta['ucd']}'"
    
    print(f"  ✓ Found metadata for {len(metadata)} columns")
    print(f"  ✓ Sample metadata for 'ra': {ra_meta}")
    return metadata


def test_get_column_metadata_gaia():
    """Test fetching column metadata for gaia_dr3 table."""
    print("\nTesting get_column_metadata('gaia_dr3')...")
    
    # Try without schema prefix
    metadata = get_column_metadata('gaia_dr3')
    
    # If empty, try with public prefix
    if not metadata:
        print("  No results without schema prefix, trying 'public.gaia_dr3'...")
        metadata = get_column_metadata('public.gaia_dr3')
    
    assert metadata, "Expected metadata for gaia_dr3, got empty dict"
    
    # Check for expected columns
    expected_cols = ['ra', 'dec', 'source_id']
    for col in expected_cols:
        assert col in metadata, f"Expected column '{col}' in metadata"
    
    # Check dec column has correct metadata
    dec_meta = metadata['dec']
    assert dec_meta['datatype'] == 'double', f"Expected dec datatype 'double', got '{dec_meta['datatype']}'"
    assert dec_meta['unit'] == 'deg', f"Expected dec unit 'deg', got '{dec_meta['unit']}'"
    assert 'pos.eq.dec' in dec_meta['ucd'], f"Expected dec ucd to contain 'pos.eq.dec', got '{dec_meta['ucd']}'"
    
    print(f"  ✓ Found metadata for {len(metadata)} columns")
    print(f"  ✓ Sample metadata for 'dec': {dec_meta}")
    return metadata


def test_get_column_metadata_nonexistent():
    """Test fetching column metadata for nonexistent table."""
    print("\nTesting get_column_metadata('nonexistent_table')...")
    
    metadata = get_column_metadata('nonexistent_table')
    
    assert metadata == {}, f"Expected empty dict for nonexistent table, got {metadata}"
    print("  ✓ Returns empty dict for nonexistent table")


def test_get_column_metadata_empty():
    """Test fetching column metadata with empty table name."""
    print("\nTesting get_column_metadata('')...")
    
    metadata = get_column_metadata('')
    
    assert metadata == {}, f"Expected empty dict for empty table name, got {metadata}"
    print("  ✓ Returns empty dict for empty table name")


def main():
    """Run all tests."""
    print("=" * 70)
    print("Column Metadata Retrieval Tests")
    print("=" * 70)
    
    try:
        test_get_column_metadata_ztf()
        test_get_column_metadata_gaia()
        test_get_column_metadata_nonexistent()
        test_get_column_metadata_empty()
        
        print("\n" + "=" * 70)
        print("All tests passed! ✓")
        print("=" * 70)
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
