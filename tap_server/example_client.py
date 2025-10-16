"""
Example TAP client demonstrating how to query the TAP server.

This script shows various ways to interact with the TAP server.
"""

import requests
import xml.etree.ElementTree as ET


def query_tap_server(query, base_url='http://localhost:5000'):
    """
    Send an ADQL query to the TAP server and return the response.
    
    Args:
        query: ADQL query string
        base_url: Base URL of the TAP server
        
    Returns:
        Response object from requests
    """
    url = f'{base_url}/sync'
    params = {
        'REQUEST': 'doQuery',
        'LANG': 'ADQL',
        'QUERY': query
    }
    
    response = requests.post(url, data=params)
    return response


def parse_votable_response(xml_content):
    """
    Parse a VOTable XML response and extract data.
    
    Args:
        xml_content: XML string content
        
    Returns:
        Tuple of (column_names, rows)
    """
    root = ET.fromstring(xml_content)
    
    # Define namespace
    ns = {'vot': 'http://www.ivoa.net/xml/VOTable/v1.4'}
    
    # Check query status
    status_info = root.find('.//vot:INFO[@name="QUERY_STATUS"]', ns)
    if status_info is not None and status_info.get('value') == 'ERROR':
        error_info = root.find('.//vot:INFO[@name="ERROR"]', ns)
        error_msg = error_info.get('value') if error_info is not None else 'Unknown error'
        raise Exception(f"Query failed: {error_msg}")
    
    # Extract column names
    fields = root.findall('.//vot:FIELD', ns)
    column_names = [field.get('name') for field in fields]
    
    # Extract rows
    rows = []
    for tr in root.findall('.//vot:TR', ns):
        row = [td.text for td in tr.findall('vot:TD', ns)]
        rows.append(row)
    
    return column_names, rows


def print_results(columns, rows):
    """Pretty print query results."""
    # Calculate column widths
    widths = [len(col) for col in columns]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))
    
    # Print header
    header = ' | '.join(col.ljust(widths[i]) for i, col in enumerate(columns))
    print(header)
    print('-' * len(header))
    
    # Print rows
    for row in rows:
        print(' | '.join(str(cell).ljust(widths[i]) for i, cell in enumerate(row)))


def main():
    """Run example queries."""
    print("TAP Client Example")
    print("=" * 70)
    
    # Example 1: Simple query with all columns
    print("\nExample 1: Select all columns")
    print("-" * 70)
    query1 = "SELECT * FROM ztf_dr14 LIMIT 3"
    print(f"Query: {query1}")
    
    response = query_tap_server(query1)
    if response.status_code == 200:
        columns, rows = parse_votable_response(response.text)
        print_results(columns, rows)
    else:
        print(f"Error: HTTP {response.status_code}")
    
    # Example 2: Select specific columns
    print("\n\nExample 2: Select specific columns")
    print("-" * 70)
    query2 = "SELECT ra, dec, mag FROM ztf_dr14 LIMIT 5"
    print(f"Query: {query2}")
    
    response = query_tap_server(query2)
    if response.status_code == 200:
        columns, rows = parse_votable_response(response.text)
        print_results(columns, rows)
    else:
        print(f"Error: HTTP {response.status_code}")
    
    # Example 3: Query with WHERE clause
    print("\n\nExample 3: Query with WHERE clause")
    print("-" * 70)
    query3 = "SELECT ra, dec, mag FROM ztf_dr14 WHERE mag < 20 LIMIT 5"
    print(f"Query: {query3}")
    
    response = query_tap_server(query3)
    if response.status_code == 200:
        columns, rows = parse_votable_response(response.text)
        print_results(columns, rows)
    else:
        print(f"Error: HTTP {response.status_code}")
    
    # Example 4: Test service endpoints
    print("\n\nExample 4: Query service capabilities")
    print("-" * 70)
    
    capabilities_response = requests.get('http://localhost:5000/capabilities')
    if capabilities_response.status_code == 200:
        print("✓ Capabilities endpoint working")
    
    tables_response = requests.get('http://localhost:5000/tables')
    if tables_response.status_code == 200:
        print("✓ Tables endpoint working")
    
    print("\n" + "=" * 70)
    print("All examples completed successfully!")


if __name__ == '__main__':
    try:
        main()
    except requests.exceptions.ConnectionError:
        print("Error: Could not connect to TAP server.")
        print("Make sure the server is running: python tap_server.py")
    except Exception as e:
        print(f"Error: {e}")
