#!/usr/bin/env python3
"""
Tests for the TAP Schema Import Tool

This test suite validates the import_tap_schema module functionality.
"""

import unittest
import tempfile
import os
from unittest.mock import Mock, MagicMock, patch
from import_tap_schema import TAPSchemaImporter
from tap_schema_db import TAPSchemaDatabase


class TestTAPSchemaImporter(unittest.TestCase):
    """Test cases for TAPSchemaImporter class."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary database for testing
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name
        
    def tearDown(self):
        """Clean up test fixtures."""
        # Remove temporary database
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
            
    def test_init(self):
        """Test importer initialization."""
        importer = TAPSchemaImporter('http://test.tap.server', self.db_path)
        self.assertEqual(importer.tap_url, 'http://test.tap.server')
        self.assertEqual(importer.db_path, self.db_path)
        self.assertIsNone(importer.service)
        self.assertIsNone(importer.db)
        
    @patch('import_tap_schema.pyvo.dal.TAPService')
    def test_connect(self, mock_tap_service):
        """Test connection to TAP service and database."""
        mock_tap_service.return_value = Mock()
        
        importer = TAPSchemaImporter('http://test.tap.server', self.db_path)
        importer.connect()
        
        # Verify TAP service was initialized
        mock_tap_service.assert_called_once_with('http://test.tap.server')
        self.assertIsNotNone(importer.service)
        
        # Verify database was initialized
        self.assertIsNotNone(importer.db)
        
        importer.close()
        
    @patch('import_tap_schema.pyvo.dal.TAPService')
    def test_context_manager(self, mock_tap_service):
        """Test using importer as context manager."""
        mock_tap_service.return_value = Mock()
        
        with TAPSchemaImporter('http://test.tap.server', self.db_path) as importer:
            self.assertIsNotNone(importer.service)
            self.assertIsNotNone(importer.db)
            
    @patch('import_tap_schema.pyvo.dal.TAPService')
    def test_query_tap_schema_table(self, mock_tap_service):
        """Test querying TAP_SCHEMA tables."""
        # Create mock result
        mock_result = Mock()
        mock_result.fieldnames = ['schema_name', 'description']
        
        # Create mock rows with masked values
        mock_row1 = {
            'schema_name': 'test_schema',
            'description': 'Test description'
        }
        mock_row2 = {
            'schema_name': 'another_schema',
            'description': Mock(mask=True)  # Simulate NULL value
        }
        
        mock_result.__iter__ = Mock(return_value=iter([mock_row1, mock_row2]))
        
        # Set up mock service
        mock_service = Mock()
        mock_service.search.return_value = mock_result
        mock_tap_service.return_value = mock_service
        
        importer = TAPSchemaImporter('http://test.tap.server', self.db_path)
        importer.connect()
        
        # Query schemas table
        results = importer.query_tap_schema_table('schemas')
        
        # Verify query was called
        mock_service.search.assert_called_once()
        call_args = mock_service.search.call_args[0][0]
        self.assertIn('SELECT * FROM TAP_SCHEMA.schemas', call_args)
        
        # Verify results
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['schema_name'], 'test_schema')
        self.assertEqual(results[0]['description'], 'Test description')
        self.assertEqual(results[1]['schema_name'], 'another_schema')
        self.assertIsNone(results[1]['description'])  # Masked value should be None
        
        importer.close()
        
    @patch('import_tap_schema.pyvo.dal.TAPService')
    def test_import_schema(self, mock_tap_service):
        """Test importing a schema."""
        # Create mock result for schema query
        mock_result = Mock()
        mock_result.fieldnames = ['schema_name', 'description', 'utype']
        mock_row = {
            'schema_name': 'test_schema',
            'description': 'Test Schema',
            'utype': None
        }
        mock_result.__iter__ = Mock(return_value=iter([mock_row]))
        
        mock_service = Mock()
        mock_service.search.return_value = mock_result
        mock_tap_service.return_value = mock_service
        
        importer = TAPSchemaImporter('http://test.tap.server', self.db_path)
        importer.connect()
        
        # Import schema
        importer.import_schema('test_schema')
        
        # Verify schema was inserted into database
        with TAPSchemaDatabase(self.db_path) as db:
            schemas = db.query("SELECT * FROM schemas WHERE schema_name = 'test_schema'")
            self.assertEqual(len(schemas), 1)
            self.assertEqual(schemas[0]['schema_name'], 'test_schema')
            self.assertEqual(schemas[0]['description'], 'Test Schema')
            
        importer.close()
        
    @patch('import_tap_schema.pyvo.dal.TAPService')
    def test_import_tables(self, mock_tap_service):
        """Test importing tables."""
        # Create mock result for tables query
        mock_result = Mock()
        mock_result.fieldnames = ['schema_name', 'table_name', 'table_type', 'description', 'utype']
        mock_rows = [
            {
                'schema_name': 'test_schema',
                'table_name': 'table1',
                'table_type': 'table',
                'description': 'First table',
                'utype': None
            },
            {
                'schema_name': 'test_schema',
                'table_name': 'table2',
                'table_type': 'table',
                'description': 'Second table',
                'utype': None
            }
        ]
        mock_result.__iter__ = Mock(return_value=iter(mock_rows))
        
        mock_service = Mock()
        mock_service.search.return_value = mock_result
        mock_tap_service.return_value = mock_service
        
        importer = TAPSchemaImporter('http://test.tap.server', self.db_path)
        importer.connect()
        
        # First insert the schema
        with TAPSchemaDatabase(self.db_path) as db:
            db.insert_schema('test_schema', 'Test Schema')
        
        # Import tables
        table_names = importer.import_tables('test_schema')
        
        # Verify tables were returned
        self.assertEqual(len(table_names), 2)
        self.assertIn('test_schema.table1', table_names)
        self.assertIn('test_schema.table2', table_names)
        
        # Verify tables were inserted into database
        with TAPSchemaDatabase(self.db_path) as db:
            tables = db.query("SELECT * FROM tables WHERE schema_name = 'test_schema'")
            self.assertEqual(len(tables), 2)
            
        importer.close()
        
    @patch('import_tap_schema.pyvo.dal.TAPService')
    def test_import_columns(self, mock_tap_service):
        """Test importing columns."""
        # Create mock result for columns query
        mock_result = Mock()
        mock_result.fieldnames = ['table_name', 'column_name', 'datatype', 'description', 
                                   'unit', 'ucd', 'utype', 'size', 'principal', 'indexed', 'std']
        mock_rows = [
            {
                'table_name': 'test_schema.table1',
                'column_name': 'col1',
                'datatype': 'double',
                'description': 'Column 1',
                'unit': 'deg',
                'ucd': 'pos.eq.ra',
                'utype': None,
                'size': None,
                'principal': 1,
                'indexed': 1,
                'std': 0
            }
        ]
        mock_result.__iter__ = Mock(return_value=iter(mock_rows))
        
        mock_service = Mock()
        mock_service.search.return_value = mock_result
        mock_tap_service.return_value = mock_service
        
        importer = TAPSchemaImporter('http://test.tap.server', self.db_path)
        importer.connect()
        
        # Import columns
        importer.import_columns(['test_schema.table1'])
        
        # Verify columns were inserted into database
        with TAPSchemaDatabase(self.db_path) as db:
            columns = db.query("SELECT * FROM columns WHERE table_name = 'test_schema.table1'")
            self.assertEqual(len(columns), 1)
            self.assertEqual(columns[0]['column_name'], 'col1')
            self.assertEqual(columns[0]['datatype'], 'double')
            self.assertEqual(columns[0]['unit'], 'deg')
            
        importer.close()


class TestSecurityFeatures(unittest.TestCase):
    """Test security features of the importer."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name
        
    def tearDown(self):
        """Clean up test fixtures."""
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
            
    @patch('import_tap_schema.pyvo.dal.TAPService')
    def test_adql_string_escaping(self, mock_tap_service):
        """Test that single quotes in schema names are properly escaped."""
        mock_result = Mock()
        mock_result.fieldnames = ['schema_name']
        mock_result.__iter__ = Mock(return_value=iter([]))
        
        mock_service = Mock()
        mock_service.search.return_value = mock_result
        mock_tap_service.return_value = mock_service
        
        importer = TAPSchemaImporter('http://test.tap.server', self.db_path)
        importer.connect()
        
        # Try to import a schema with a single quote (potential SQL injection)
        schema_name = "test'schema"
        importer.import_schema(schema_name)
        
        # Verify the query was called with escaped quotes
        mock_service.search.assert_called()
        call_args = mock_service.search.call_args[0][0]
        # Single quote should be doubled for ADQL/SQL escaping
        self.assertIn("test''schema", call_args)
        # Verify unescaped version is not present (except as part of escaped version)
        self.assertNotIn("test'schema'", call_args)
        
        importer.close()
        
    @patch('import_tap_schema.pyvo.dal.TAPService')
    def test_table_name_validation(self, mock_tap_service):
        """Test that invalid table names are rejected."""
        # Set up mock result for valid queries
        mock_result = Mock()
        mock_result.fieldnames = ['test']
        mock_result.__iter__ = Mock(return_value=iter([]))
        
        mock_service = Mock()
        mock_service.search.return_value = mock_result
        mock_tap_service.return_value = mock_service
        
        importer = TAPSchemaImporter('http://test.tap.server', self.db_path)
        importer.connect()
        
        # Try to query with an invalid table name (potential injection)
        with self.assertRaises(ValueError):
            importer.query_tap_schema_table('tables; DROP TABLE schemas--')
        
        with self.assertRaises(ValueError):
            importer.query_tap_schema_table('../etc/passwd')
        
        # Valid table names should work
        try:
            importer.query_tap_schema_table('tables')
            importer.query_tap_schema_table('key_columns')
        except ValueError:
            self.fail("Valid table names should not raise ValueError")
        
        importer.close()


class TestTableImport(unittest.TestCase):
    """Test importing by table name."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name
        
    def tearDown(self):
        """Clean up test fixtures."""
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
            
    @patch('import_tap_schema.pyvo.dal.TAPService')
    def test_import_table_by_name(self, mock_tap_service):
        """Test importing a specific table by name."""
        # Mock table lookup result
        mock_table_result = Mock()
        mock_table_result.fieldnames = ['schema_name', 'table_name', 'table_type', 'description', 'utype']
        mock_table_row = {
            'schema_name': 'gaia_dr3',
            'table_name': 'gaia_dr3_source',
            'table_type': 'table',
            'description': 'Gaia DR3 source catalog',
            'utype': None
        }
        mock_table_result.__iter__ = Mock(return_value=iter([mock_table_row]))
        
        # Mock schema lookup result
        mock_schema_result = Mock()
        mock_schema_result.fieldnames = ['schema_name', 'description', 'utype']
        mock_schema_row = {
            'schema_name': 'gaia_dr3',
            'description': 'Gaia Data Release 3',
            'utype': None
        }
        mock_schema_result.__iter__ = Mock(return_value=iter([mock_schema_row]))
        
        # Mock columns result
        # Note: TAP servers may store table_name without schema prefix in columns table
        mock_columns_result = Mock()
        mock_columns_result.fieldnames = ['table_name', 'column_name', 'datatype', 'description']
        mock_column_rows = [
            {
                'table_name': 'gaia_dr3_source',
                'column_name': 'source_id',
                'datatype': 'long',
                'description': 'Unique source identifier'
            }
        ]
        mock_columns_result.__iter__ = Mock(return_value=iter(mock_column_rows))
        
        # Set up mock service to return different results based on query
        mock_service = Mock()
        def search_side_effect(query):
            if 'TAP_SCHEMA.tables' in query and 'gaia_dr3_source' in query:
                return mock_table_result
            elif 'TAP_SCHEMA.schemas' in query:
                return mock_schema_result
            elif 'TAP_SCHEMA.columns' in query:
                return mock_columns_result
            else:
                # Return empty result for keys queries
                empty_result = Mock()
                empty_result.fieldnames = []
                empty_result.__iter__ = Mock(return_value=iter([]))
                return empty_result
        
        mock_service.search.side_effect = search_side_effect
        mock_tap_service.return_value = mock_service
        
        importer = TAPSchemaImporter('http://test.tap.server', self.db_path)
        importer.connect()
        
        # Import table by name
        success = importer.import_table_by_name('gaia_dr3_source', include_keys=False)
        
        # Verify success
        self.assertTrue(success)
        
        # Verify table was inserted into database
        with TAPSchemaDatabase(self.db_path) as db:
            tables = db.query("SELECT * FROM tables WHERE table_name = 'gaia_dr3_source'")
            self.assertEqual(len(tables), 1)
            self.assertEqual(tables[0]['schema_name'], 'gaia_dr3')
            self.assertEqual(tables[0]['table_name'], 'gaia_dr3_source')
            
            # Verify columns were inserted
            # Columns are stored with table_name as returned by TAP server
            columns = db.query("SELECT * FROM columns WHERE table_name = 'gaia_dr3_source'")
            self.assertEqual(len(columns), 1)
            self.assertEqual(columns[0]['column_name'], 'source_id')
        
        importer.close()


class TestDatabaseOperations(unittest.TestCase):
    """Test database operations used by the importer."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name
        
    def tearDown(self):
        """Clean up test fixtures."""
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
            
    def test_database_initialization(self):
        """Test that database is properly initialized."""
        with TAPSchemaDatabase(self.db_path) as db:
            db.initialize_schema()
            
            # Verify all tables exist
            result = db.query("""
                SELECT name FROM sqlite_master 
                WHERE type='table' 
                ORDER BY name
            """)
            
            table_names = [row['name'] for row in result]
            self.assertIn('schemas', table_names)
            self.assertIn('tables', table_names)
            self.assertIn('columns', table_names)
            self.assertIn('keys', table_names)
            self.assertIn('key_columns', table_names)
            
    def test_insert_and_query(self):
        """Test inserting and querying data."""
        with TAPSchemaDatabase(self.db_path) as db:
            db.initialize_schema()
            
            # Insert test data
            db.insert_schema('test', 'Test Schema')
            db.insert_table('test', 'mytable', 'table', 'My Table')
            db.insert_column('test.mytable', 'mycolumn', 
                           datatype='double', unit='deg', description='My Column')
            
            # Query and verify
            schemas = db.query("SELECT * FROM schemas")
            self.assertEqual(len(schemas), 1)
            self.assertEqual(schemas[0]['schema_name'], 'test')
            
            tables = db.query("SELECT * FROM tables")
            self.assertEqual(len(tables), 1)
            self.assertEqual(tables[0]['table_name'], 'mytable')
            
            columns = db.query("SELECT * FROM columns")
            self.assertEqual(len(columns), 1)
            self.assertEqual(columns[0]['column_name'], 'mycolumn')


if __name__ == '__main__':
    unittest.main()
