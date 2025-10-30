#!/usr/bin/env python
"""
Demonstration script showing how column metadata is retrieved from tap_schema.db
and used in VOTable responses.
"""

import sys
import os
import xml.etree.ElementTree as ET

# Add the tap_server directory to the path
sys.path.insert(0, os.path.dirname(__file__))

from tap_schema_db import TAPSchemaDatabase


def demonstrate_metadata_retrieval():
    """Demonstrate retrieving column metadata from tap_schema.db."""
    print("=" * 70)
    print("Column Metadata Retrieval Demonstration")
    print("=" * 70)
    print()
    
    db_path = os.path.join(os.path.dirname(__file__), 'tap_schema.db')
    
    # Example 1: Query for ztf_dr14 columns
    print("Example 1: Retrieving metadata for public.ztf_dr14")
    print("-" * 70)
    
    with TAPSchemaDatabase(db_path) as db:
        query = """
            SELECT column_name, datatype, unit, ucd, description 
            FROM columns 
            WHERE table_name = 'public.ztf_dr14'
            ORDER BY column_name
        """
        results = db.query(query)
        
        print(f"Found {len(results)} columns:\n")
        for row in results:
            print(f"  Column: {row['column_name']}")
            print(f"    datatype: {row['datatype']}")
            print(f"    unit:     {row['unit']}")
            print(f"    ucd:      {row['ucd']}")
            print(f"    desc:     {row['description']}")
            print()
    
    # Example 2: Query for gaia_dr3 columns
    print("\nExample 2: Retrieving metadata for public.gaia_dr3")
    print("-" * 70)
    
    with TAPSchemaDatabase(db_path) as db:
        query = """
            SELECT column_name, datatype, unit, ucd, description 
            FROM columns 
            WHERE table_name = 'public.gaia_dr3'
            ORDER BY column_name
        """
        results = db.query(query)
        
        print(f"Found {len(results)} columns:\n")
        for row in results:
            print(f"  Column: {row['column_name']}")
            print(f"    datatype: {row['datatype']}")
            print(f"    unit:     {row['unit']}")
            print(f"    ucd:      {row['ucd']}")
            print(f"    desc:     {row['description']}")
            print()


def demonstrate_votable_field_generation():
    """Demonstrate how metadata is used in VOTable FIELD elements."""
    print("\n" + "=" * 70)
    print("VOTable FIELD Generation with Metadata")
    print("=" * 70)
    print()
    
    db_path = os.path.join(os.path.dirname(__file__), 'tap_schema.db')
    
    # Get metadata for ztf_dr14
    with TAPSchemaDatabase(db_path) as db:
        query = """
            SELECT column_name, datatype, unit, ucd 
            FROM columns 
            WHERE table_name = 'public.ztf_dr14'
        """
        results = db.query(query)
    
    # Create a simple VOTable structure
    votable = ET.Element('VOTABLE', {'version': '1.4'})
    resource = ET.SubElement(votable, 'RESOURCE')
    table = ET.SubElement(resource, 'TABLE', {'name': 'ztf_dr14'})
    
    print("Generated VOTable FIELD elements:")
    print("-" * 70)
    print()
    
    # Generate FIELD elements
    for row in results:
        field_attrs = {
            'name': row['column_name'],
            'datatype': row['datatype'] if row['datatype'] else 'char'
        }
        
        if row['unit']:
            field_attrs['unit'] = row['unit']
        if row['ucd']:
            field_attrs['ucd'] = row['ucd']
        
        field = ET.SubElement(table, 'FIELD', field_attrs)
        
        # Pretty print the FIELD element
        field_str = ET.tostring(field, encoding='unicode')
        print(f"  {field_str}")
    
    print()
    print("=" * 70)
    print("Summary")
    print("=" * 70)
    print()
    print("The tap_server.py now:")
    print("  1. Queries tap_schema.db using the table name from the ADQL query")
    print("  2. Retrieves column metadata (datatype, unit, ucd, description)")
    print("  3. Uses this metadata to generate accurate VOTable FIELD elements")
    print("  4. Falls back to hard-coded values if metadata is not available")
    print()


def main():
    """Run the demonstration."""
    demonstrate_metadata_retrieval()
    demonstrate_votable_field_generation()


if __name__ == '__main__':
    main()
