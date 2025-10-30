#!/usr/bin/env python3

"""
Test script for import_tap_schema.py

This script tests the database creation and metadata insertion functionality
without requiring external network access to TAP servers.
"""

import os
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch, MagicMock

# Add the bin directory to the path
sys.path.insert(0, os.path.dirname(__file__))

from import_tap_schema import TAPSchemaImporter


class TestTAPSchemaImporter(unittest.TestCase):
    """Test the TAPSchemaImporter class."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary database file
        self.temp_db = tempfile.NamedTemporaryFile(mode='w', suffix='.db', delete=False)
        self.temp_db.close()
        self.db_path = self.temp_db.name

    def tearDown(self):
        """Clean up test fixtures."""
        # Remove temporary database file
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)

    def test_database_initialization(self):
        """Test that the database is initialized with correct tables."""
        importer = TAPSchemaImporter(db_path=self.db_path)

        # Verify database file was created
        self.assertTrue(os.path.exists(self.db_path))

        # Check that tables were created
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        # Check schemas table
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='schemas'")
        self.assertIsNotNone(cur.fetchone())

        # Check tables table
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tables'")
        self.assertIsNotNone(cur.fetchone())

        # Check columns table
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='columns'")
        self.assertIsNotNone(cur.fetchone())

        conn.close()

    def test_insert_metadata(self):
        """Test inserting metadata into the database."""
        importer = TAPSchemaImporter(db_path=self.db_path)

        # Sample metadata
        schemas_data = [
            {'schema_name': 'test_schema', 'description': 'Test schema description'}
        ]

        tables_data = [
            {
                'table_name': 'test_schema.test_table',
                'schema_name': 'test_schema',
                'description': 'Test table description'
            }
        ]

        columns_data = [
            {
                'table_name': 'test_schema.test_table',
                'column_name': 'col1',
                'ucd': 'pos.eq.ra',
                'unit': 'deg',
                'datatype': 'DOUBLE',
                'description': 'Right ascension'
            },
            {
                'table_name': 'test_schema.test_table',
                'column_name': 'col2',
                'ucd': 'pos.eq.dec',
                'unit': 'deg',
                'datatype': 'DOUBLE',
                'description': 'Declination'
            }
        ]

        # Insert metadata
        importer.insert_metadata(schemas_data, tables_data, columns_data)

        # Verify data was inserted
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        # Check schemas
        cur.execute("SELECT COUNT(*) FROM schemas")
        self.assertEqual(cur.fetchone()[0], 1)

        # Check tables
        cur.execute("SELECT COUNT(*) FROM tables")
        self.assertEqual(cur.fetchone()[0], 1)

        # Check columns
        cur.execute("SELECT COUNT(*) FROM columns")
        self.assertEqual(cur.fetchone()[0], 2)

        # Verify specific data
        cur.execute("SELECT schema_name, schema_description FROM schemas WHERE schema_name='test_schema'")
        row = cur.fetchone()
        self.assertEqual(row[0], 'test_schema')
        self.assertEqual(row[1], 'Test schema description')

        cur.execute("SELECT table_name, schema_name FROM tables WHERE table_name='test_schema.test_table'")
        row = cur.fetchone()
        self.assertEqual(row[0], 'test_schema.test_table')
        self.assertEqual(row[1], 'test_schema')

        cur.execute("SELECT column_name, ucd, unit FROM columns WHERE table_name='test_schema.test_table' ORDER BY column_name")
        rows = cur.fetchall()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][0], 'col1')
        self.assertEqual(rows[0][1], 'pos.eq.ra')
        self.assertEqual(rows[0][2], 'deg')

        conn.close()

    def test_replace_existing_metadata(self):
        """Test that INSERT OR REPLACE works correctly."""
        importer = TAPSchemaImporter(db_path=self.db_path)

        # Insert initial data
        schemas_data = [
            {'schema_name': 'test_schema', 'description': 'Old description'}
        ]
        importer.insert_metadata(schemas_data, [], [])

        # Insert updated data
        schemas_data = [
            {'schema_name': 'test_schema', 'description': 'New description'}
        ]
        importer.insert_metadata(schemas_data, [], [])

        # Verify updated data
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute("SELECT schema_description FROM schemas WHERE schema_name='test_schema'")
        row = cur.fetchone()
        self.assertEqual(row[0], 'New description')
        conn.close()

    @patch('import_tap_schema.pyvo.dal.TAPService')
    def test_query_schemas(self, mock_tap_service):
        """Test querying schemas from a TAP service."""
        importer = TAPSchemaImporter(db_path=self.db_path)

        # Mock the TAP service response
        mock_service = Mock()
        mock_result = [
            {'schema_name': 'schema1', 'description': 'Schema 1'},
            {'schema_name': 'schema2', 'description': 'Schema 2'}
        ]
        mock_service.search.return_value = mock_result
        mock_tap_service.return_value = mock_service

        # Query schemas
        schemas = importer._query_schemas(mock_service)

        # Verify results
        self.assertEqual(len(schemas), 2)
        self.assertEqual(schemas[0]['schema_name'], 'schema1')
        self.assertEqual(schemas[1]['schema_name'], 'schema2')

    @patch('import_tap_schema.pyvo.dal.TAPService')
    def test_query_tables(self, mock_tap_service):
        """Test querying tables from a TAP service."""
        importer = TAPSchemaImporter(db_path=self.db_path)

        # Mock the TAP service response
        mock_service = Mock()
        mock_result = [
            {
                'table_name': 'schema1.table1',
                'schema_name': 'schema1',
                'description': 'Table 1'
            }
        ]
        mock_service.search.return_value = mock_result
        mock_tap_service.return_value = mock_service

        # Query tables
        tables = importer._query_tables(mock_service, schema_name='schema1')

        # Verify results
        self.assertEqual(len(tables), 1)
        self.assertEqual(tables[0]['table_name'], 'schema1.table1')
        self.assertEqual(tables[0]['schema_name'], 'schema1')

    @patch('import_tap_schema.pyvo.dal.TAPService')
    def test_query_columns(self, mock_tap_service):
        """Test querying columns from a TAP service."""
        importer = TAPSchemaImporter(db_path=self.db_path)

        # Mock the TAP service response
        mock_service = Mock()
        mock_result = [
            {
                'table_name': 'schema1.table1',
                'column_name': 'ra',
                'ucd': 'pos.eq.ra',
                'unit': 'deg',
                'datatype': 'DOUBLE',
                'description': 'Right ascension'
            }
        ]
        mock_service.search.return_value = mock_result
        mock_tap_service.return_value = mock_service

        # Query columns
        columns = importer._query_columns(mock_service, table_name='schema1.table1')

        # Verify results
        self.assertEqual(len(columns), 1)
        self.assertEqual(columns[0]['column_name'], 'ra')
        self.assertEqual(columns[0]['ucd'], 'pos.eq.ra')


def main():
    """Run tests."""
    unittest.main(verbosity=2)


if __name__ == '__main__':
    main()
