"""
Simple tests for the ADQL parser module.
"""

from adql_to_lsdb import parse_adql, adql_to_lsdb, ADQLParser


def test_basic_select():
    """Test parsing a basic SELECT query."""
    query = "SELECT * FROM ztf_dr14"
    result = parse_adql(query)
    
    assert result['table'] == 'ztf_dr14'
    assert result['columns'] == ['*']
    assert result['where'] is None
    assert result['limit'] is None
    print("✓ test_basic_select passed")


def test_select_with_columns():
    """Test parsing SELECT with specific columns."""
    query = "SELECT ra, dec, mag FROM ztf_dr14"
    result = parse_adql(query)
    
    assert result['table'] == 'ztf_dr14'
    assert result['columns'] == ['ra', 'dec', 'mag']
    assert result['where'] is None
    assert result['limit'] is None
    print("✓ test_select_with_columns passed")


def test_select_with_where():
    """Test parsing SELECT with WHERE clause."""
    query = "SELECT ra, dec FROM ztf_dr14 WHERE mag < 20"
    result = parse_adql(query)
    
    assert result['table'] == 'ztf_dr14'
    assert result['columns'] == ['ra', 'dec']
    assert result['where'] == 'mag < 20'
    assert result['limit'] is None
    print("✓ test_select_with_where passed")


def test_select_with_limit():
    """Test parsing SELECT with LIMIT."""
    query = "SELECT * FROM ztf_dr14 LIMIT 100"
    result = parse_adql(query)
    
    assert result['table'] == 'ztf_dr14'
    assert result['columns'] == ['*']
    assert result['where'] is None
    assert result['limit'] == 100
    print("✓ test_select_with_limit passed")


def test_complex_query():
    """Test parsing a complex query with multiple clauses."""
    query = "SELECT ra, dec, mag FROM ztf_dr14 WHERE mag < 20 LIMIT 50"
    result = parse_adql(query)
    
    assert result['table'] == 'ztf_dr14'
    assert result['columns'] == ['ra', 'dec', 'mag']
    assert result['where'] == 'mag < 20'
    assert result['limit'] == 50
    print("✓ test_complex_query passed")


def test_case_insensitive():
    """Test that parsing is case-insensitive."""
    query = "select ra, dec from ztf_dr14 where mag < 20 limit 10"
    result = parse_adql(query)
    
    assert result['table'] == 'ztf_dr14'
    assert result['columns'] == ['ra', 'dec']
    assert result['where'] == 'mag < 20'
    assert result['limit'] == 10
    print("✓ test_case_insensitive passed")


def test_to_lsdb_code():
    """Test generating LSDB code from ADQL."""
    query = "SELECT ra, dec, mag FROM ztf_dr14 WHERE mag < 20 LIMIT 100"
    code = adql_to_lsdb(query)
    
    # Check that code contains expected elements
    assert 'import lsdb' in code
    assert 'ztf_dr14' in code
    assert 'mag < 20' in code
    assert '100' in code
    print("✓ test_to_lsdb_code passed")


def test_whitespace_handling():
    """Test that extra whitespace is handled correctly."""
    query = "SELECT   ra,   dec   FROM    ztf_dr14    WHERE   mag   <   20"
    result = parse_adql(query)
    
    assert result['table'] == 'ztf_dr14'
    assert len(result['columns']) == 2
    assert result['where'] is not None
    print("✓ test_whitespace_handling passed")


if __name__ == '__main__':
    print("Running ADQL Parser Tests...\n")
    
    test_basic_select()
    test_select_with_columns()
    test_select_with_where()
    test_select_with_limit()
    test_complex_query()
    test_case_insensitive()
    test_to_lsdb_code()
    test_whitespace_handling()
    
    print("\nAll tests passed! ✓")
