#!/usr/bin/env python
"""
Test script for the /tables endpoint.
Verifies that the endpoint returns table metadata from tap_schema.db.
"""

import unittest
import tempfile
import os
import sys
import xml.etree.ElementTree as ET

# Add the tap_server directory to the path
sys.path.insert(0, os.path.dirname(__file__))

from tap_schema_db import TAPSchemaDatabase


class TestTablesEndpoint(unittest.TestCase):
    """Test the tables endpoint and generate_tables_xml function."""
    
    def setUp(self):
        """Create a temporary database for testing."""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name
        
        # Initialize database with test data
        with TAPSchemaDatabase(self.db_path) as db:
            db.initialize_schema()
            
            # Add test schema
            db.insert_schema('test_schema', 'Test Schema Description')
            
            # Add test table
            db.insert_table('test_schema', 'test_table', 'table', 'Test Table Description')
            
            # Add test columns
            db.insert_column('test_schema.test_table', 'col1',
                           datatype='double', unit='deg', ucd='pos.eq.ra',
                           description='Column 1 description')
            db.insert_column('test_schema.test_table', 'col2',
                           datatype='long', description='Column 2 description')
    
    def tearDown(self):
        """Clean up temporary database."""
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
    
    def test_generate_tables_xml_structure(self):
        """Test that generate_tables_xml creates valid XML."""
        # Import the function (need to patch the global tap_schema_db first)
        from tap_server import generate_tables_xml, tap_schema_db as global_db
        
        # Temporarily replace the global database with our test database
        original_path = global_db.db_path
        global_db.db_path = self.db_path
        global_db.connection = None  # Force reconnect
        
        try:
            xml_str = generate_tables_xml()
            
            # Parse XML to verify it's valid
            root = ET.fromstring(xml_str)
            
            # Define namespace
            ns = {'vod': 'http://www.ivoa.net/xml/VODataService/v1.1'}
            
            # Verify root element (handle namespace)
            self.assertTrue(root.tag.endswith('tableset'))
            # Verify XML declaration is present
            self.assertTrue(xml_str.startswith('<?xml version="1.0" encoding="UTF-8"?>'))
            
            # Find schemas
            schemas = root.findall('vod:schema', ns)
            self.assertGreater(len(schemas), 0, "Should have at least one schema")
            
            # Find our test schema
            test_schema = None
            for schema in schemas:
                name_elem = schema.find('vod:name', ns)
                if name_elem is not None and name_elem.text == 'test_schema':
                    test_schema = schema
                    break
            
            self.assertIsNotNone(test_schema, "Test schema should be present")
            
            # Verify schema description
            desc_elem = test_schema.find('vod:description', ns)
            self.assertIsNotNone(desc_elem)
            self.assertEqual(desc_elem.text, 'Test Schema Description')
            
            # Find test table
            tables = test_schema.findall('vod:table', ns)
            self.assertEqual(len(tables), 1, "Should have exactly one table")
            
            test_table = tables[0]
            table_name_elem = test_table.find('vod:name', ns)
            self.assertEqual(table_name_elem.text, 'test_table')
            
            # Verify table description
            table_desc_elem = test_table.find('vod:description', ns)
            self.assertEqual(table_desc_elem.text, 'Test Table Description')
            
            # Find columns
            columns = test_table.findall('vod:column', ns)
            self.assertEqual(len(columns), 2, "Should have exactly two columns")
            
            # Verify first column (col1)
            col1 = columns[0]
            self.assertEqual(col1.find('vod:name', ns).text, 'col1')
            self.assertEqual(col1.find('vod:dataType', ns).text, 'double')
            self.assertEqual(col1.find('vod:unit', ns).text, 'deg')
            self.assertEqual(col1.find('vod:ucd', ns).text, 'pos.eq.ra')
            self.assertEqual(col1.find('vod:description', ns).text, 'Column 1 description')
            
            # Verify second column (col2)
            col2 = columns[1]
            self.assertEqual(col2.find('vod:name', ns).text, 'col2')
            self.assertEqual(col2.find('vod:dataType', ns).text, 'long')
            self.assertEqual(col2.find('vod:description', ns).text, 'Column 2 description')
            # col2 should not have unit or ucd
            self.assertIsNone(col2.find('vod:unit', ns))
            self.assertIsNone(col2.find('vod:ucd', ns))
            
        finally:
            # Restore original database path
            global_db.db_path = original_path
            global_db.connection = None
    
    def test_tables_endpoint_with_flask(self):
        """Test the /tables endpoint using Flask test client."""
        from tap_server import app, tap_schema_db as global_db
        
        # Temporarily replace the global database with our test database
        original_path = global_db.db_path
        global_db.db_path = self.db_path
        global_db.connection = None
        
        try:
            with app.test_client() as client:
                response = client.get('/tables')
                
                # Verify response
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.mimetype, 'application/xml')
                
                # Parse and verify XML
                root = ET.fromstring(response.data)
                
                # Define namespace
                ns = {'vod': 'http://www.ivoa.net/xml/VODataService/v1.1'}
                
                # Verify root element
                self.assertTrue(root.tag.endswith('tableset'))
                
                # Verify test schema is present
                schemas = root.findall('vod:schema', ns)
                schema_names = [s.find('vod:name', ns).text for s in schemas]
                self.assertIn('test_schema', schema_names)
                
        finally:
            # Restore original database path
            global_db.db_path = original_path
            global_db.connection = None


if __name__ == '__main__':
    unittest.main()
