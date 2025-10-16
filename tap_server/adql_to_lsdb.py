"""
ADQL to LSDB Query Converter

This module provides functionality to parse ADQL (Astronomical Data Query Language)
queries and convert them into LSDB-compatible operations.

This is a prototype implementation that supports basic ADQL syntax.
"""

import re
from typing import Dict, List, Optional, Tuple


class ADQLParser:
    """Parser for ADQL queries that converts them to LSDB operations."""
    
    def __init__(self):
        self.table_name = None
        self.columns = []
        self.where_clause = None
        self.limit = None
        
    def parse(self, adql_query: str) -> Dict:
        """
        Parse an ADQL query and extract components.
        
        Args:
            adql_query: The ADQL query string
            
        Returns:
            Dictionary containing parsed query components
        """
        # Normalize the query
        query = adql_query.strip()
        query = re.sub(r'\s+', ' ', query)  # Normalize whitespace
        
        # Extract SELECT clause
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', query, re.IGNORECASE)
        if select_match:
            columns_str = select_match.group(1).strip()
            if columns_str == '*':
                self.columns = ['*']
            else:
                self.columns = [col.strip() for col in columns_str.split(',')]
        
        # Extract FROM clause (table name)
        from_match = re.search(r'FROM\s+(\w+)', query, re.IGNORECASE)
        if from_match:
            self.table_name = from_match.group(1)
        
        # Extract WHERE clause
        where_match = re.search(r'WHERE\s+(.*?)(?:\s+LIMIT|\s*$)', query, re.IGNORECASE)
        if where_match:
            self.where_clause = where_match.group(1).strip()
        
        # Extract LIMIT clause
        limit_match = re.search(r'LIMIT\s+(\d+)', query, re.IGNORECASE)
        if limit_match:
            self.limit = int(limit_match.group(1))
        
        return {
            'table': self.table_name,
            'columns': self.columns,
            'where': self.where_clause,
            'limit': self.limit
        }
    
    def to_lsdb_code(self, adql_query: str) -> str:
        """
        Convert ADQL query to LSDB Python code.
        
        Args:
            adql_query: The ADQL query string
            
        Returns:
            String containing Python code for LSDB operations
        """
        parsed = self.parse(adql_query)
        
        code_lines = []
        code_lines.append("import lsdb")
        code_lines.append("")
        
        # Load catalog
        if parsed['table']:
            code_lines.append(f"# Load catalog: {parsed['table']}")
            code_lines.append(f"catalog = lsdb.read_hipscat('{parsed['table']}')")
        else:
            code_lines.append("# No table specified")
            code_lines.append("catalog = None")
        
        # Apply WHERE clause if present
        if parsed['where']:
            code_lines.append("")
            code_lines.append(f"# Apply filter: {parsed['where']}")
            code_lines.append(f"# filtered = catalog.query('{parsed['where']}')")
            code_lines.append("# Note: Complex WHERE clauses may need manual conversion")
        
        # Select columns if not '*'
        if parsed['columns'] and parsed['columns'] != ['*']:
            code_lines.append("")
            code_lines.append(f"# Select columns: {', '.join(parsed['columns'])}")
            code_lines.append(f"# result = catalog[{parsed['columns']}]")
        
        # Apply LIMIT if present
        if parsed['limit']:
            code_lines.append("")
            code_lines.append(f"# Limit results to {parsed['limit']} rows")
            code_lines.append(f"# result = catalog.head({parsed['limit']})")
        
        code_lines.append("")
        code_lines.append("# Execute and get results")
        code_lines.append("# df = result.compute()")
        
        return '\n'.join(code_lines)


def parse_adql(adql_query: str) -> Dict:
    """
    Parse an ADQL query and return its components.
    
    Args:
        adql_query: The ADQL query string
        
    Returns:
        Dictionary containing parsed query components
    """
    parser = ADQLParser()
    return parser.parse(adql_query)


def adql_to_lsdb(adql_query: str) -> str:
    """
    Convert an ADQL query to LSDB Python code.
    
    Args:
        adql_query: The ADQL query string
        
    Returns:
        String containing Python code for LSDB operations
    """
    parser = ADQLParser()
    return parser.to_lsdb_code(adql_query)


if __name__ == "__main__":
    # Example usage
    example_query = "SELECT ra, dec, mag FROM ztf_dr14 WHERE mag < 20 LIMIT 100"
    
    print("Example ADQL Query:")
    print(example_query)
    print("\n" + "="*50 + "\n")
    
    print("Parsed Components:")
    parsed = parse_adql(example_query)
    for key, value in parsed.items():
        print(f"  {key}: {value}")
    
    print("\n" + "="*50 + "\n")
    
    print("Generated LSDB Code:")
    print(adql_to_lsdb(example_query))
