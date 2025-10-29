"""
TAP Schema Database Module

This module provides a SQLite-based storage layer for TAP_SCHEMA metadata tables
as defined in the IVOA TAP 1.1 specification (Section 4.3).

The database stores metadata about schemas, tables, columns, foreign keys, and
key columns. It uses Python's built-in sqlite3 module for maximum portability.

Reference: https://www.ivoa.net/documents/TAP/20181024/PR-TAP-1.1-20181024.html
"""

import sqlite3
import os
from typing import List, Dict, Any, Optional, Tuple


class TAPSchemaDatabase:
    """
    Database operations class for TAP_SCHEMA metadata storage.
    
    This class provides methods to:
    - Initialize the TAP_SCHEMA database schema
    - Insert, update, and delete metadata records
    - Query metadata with SQL (including JOINs)
    - Manage the SQLite database file
    
    The database file is stored on disk and can be inspected with standard
    SQLite tools (CLI: sqlite3, GUI: DB Browser for SQLite, etc.)
    """
    
    def __init__(self, db_path: str = 'tap_schema.db', qualified=''):
        """
        Initialize the TAP schema database connection.
        
        Args:
            db_path: Path to the SQLite database file. If the file doesn't exist,
                    it will be created when initialize_schema() is called.
            qualified: attach to this database qualified as this identifier,
                    if non-blank.
        """
        self.db_path = db_path
        self.qualified = qualified
        self.connection = None
        
    def connect(self):
        """Open a connection to the database."""
        if self.connection is None:
            self.connection = sqlite3.connect(self.db_path)
            # Enable foreign key constraints
            self.connection.execute('PRAGMA foreign_keys = ON')
            if self.qualified:
                # Attach as TAP_SCHEMA for conformance with protocol
                self.connection.execute("ATTACH DATABASE ? as ?;", (self.db_path, self.qualified))
            # Use Row factory for dictionary-like access
            self.connection.row_factory = sqlite3.Row
            
    def close(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        
    def initialize_schema(self):
        """
        Create the TAP_SCHEMA tables in the database.
        
        This method creates the following tables according to TAP 1.1 spec:
        - schemas: List of schemas in the TAP service
        - tables: List of tables in the TAP service
        - columns: List of columns in all tables
        - keys: Foreign key relationships
        - key_columns: Columns participating in foreign keys
        
        If tables already exist, this method does nothing (idempotent).
        """
        self.connect()
        cursor = self.connection.cursor()
        
        # Create schemas table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS schemas (
                schema_name TEXT PRIMARY KEY,
                description TEXT,
                utype TEXT
            )
        ''')
        
        # Create tables table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tables (
                schema_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                table_type TEXT,
                description TEXT,
                utype TEXT,
                PRIMARY KEY (schema_name, table_name),
                FOREIGN KEY (schema_name) REFERENCES schemas(schema_name)
            )
        ''')
        
        # Create indexes for common queries
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_tables_schema_name 
            ON tables(schema_name)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_tables_table_name 
            ON tables(table_name)
        ''')
        
        # Create columns table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS columns (
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                description TEXT,
                unit TEXT,
                ucd TEXT,
                utype TEXT,
                datatype TEXT,
                size INTEGER,
                principal INTEGER,
                indexed INTEGER,
                std INTEGER,
                PRIMARY KEY (table_name, column_name)
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_columns_table_name 
            ON columns(table_name)
        ''')
        
        # Create keys table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS keys (
                key_id TEXT PRIMARY KEY,
                from_table TEXT NOT NULL,
                target_table TEXT NOT NULL,
                description TEXT,
                utype TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_keys_from_table 
            ON keys(from_table)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_keys_target_table 
            ON keys(target_table)
        ''')
        
        # Create key_columns table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS key_columns (
                key_id TEXT NOT NULL,
                from_column TEXT NOT NULL,
                target_column TEXT NOT NULL,
                PRIMARY KEY (key_id, from_column),
                FOREIGN KEY (key_id) REFERENCES keys(key_id)
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_key_columns_key_id 
            ON key_columns(key_id)
        ''')
        
        self.connection.commit()
        
    def insert_schema(self, schema_name: str, description: str = None, utype: str = None):
        """
        Insert a schema record.
        
        Args:
            schema_name: Name of the schema
            description: Optional description
            utype: Optional UType
        """
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(
            'INSERT OR REPLACE INTO schemas (schema_name, description, utype) VALUES (?, ?, ?)',
            (schema_name, description, utype)
        )
        self.connection.commit()
        
    def insert_table(self, schema_name: str, table_name: str, table_type: str = 'table',
                     description: str = None, utype: str = None):
        """
        Insert a table record.
        
        Args:
            schema_name: Schema containing the table
            table_name: Name of the table
            table_type: Type of table ('table' or 'view')
            description: Optional description
            utype: Optional UType
        """
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(
            '''INSERT OR REPLACE INTO tables 
               (schema_name, table_name, table_type, description, utype) 
               VALUES (?, ?, ?, ?, ?)''',
            (schema_name, table_name, table_type, description, utype)
        )
        self.connection.commit()
        
    def insert_column(self, table_name: str, column_name: str, **kwargs):
        """
        Insert a column record.
        
        Args:
            table_name: Fully qualified table name (schema.table)
            column_name: Name of the column
            **kwargs: Optional column metadata (description, unit, ucd, utype, 
                     datatype, size, principal, indexed, std)
        """
        self.connect()
        cursor = self.connection.cursor()
        
        # Build column list and values
        columns = ['table_name', 'column_name']
        values = [table_name, column_name]
        
        valid_fields = ['description', 'unit', 'ucd', 'utype', 'datatype', 
                       'size', 'principal', 'indexed', 'std']
        
        for field in valid_fields:
            if field in kwargs:
                columns.append(field)
                values.append(kwargs[field])
        
        placeholders = ', '.join(['?' for _ in values])
        column_names = ', '.join(columns)
        
        cursor.execute(
            f'INSERT OR REPLACE INTO columns ({column_names}) VALUES ({placeholders})',
            values
        )
        self.connection.commit()
        
    def insert_key(self, key_id: str, from_table: str, target_table: str,
                   description: str = None, utype: str = None):
        """
        Insert a foreign key record.
        
        Args:
            key_id: Unique identifier for the key
            from_table: Source table name
            target_table: Target table name
            description: Optional description
            utype: Optional UType
        """
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(
            '''INSERT OR REPLACE INTO keys 
               (key_id, from_table, target_table, description, utype) 
               VALUES (?, ?, ?, ?, ?)''',
            (key_id, from_table, target_table, description, utype)
        )
        self.connection.commit()
        
    def insert_key_column(self, key_id: str, from_column: str, target_column: str):
        """
        Insert a key column record.
        
        Args:
            key_id: Foreign key identifier
            from_column: Column name in source table
            target_column: Column name in target table
        """
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(
            '''INSERT OR REPLACE INTO key_columns 
               (key_id, from_column, target_column) 
               VALUES (?, ?, ?)''',
            (key_id, from_column, target_column)
        )
        self.connection.commit()
        
    def query(self, sql: str, params: Tuple = None) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results as a list of dictionaries.
        
        This method supports arbitrary SQL queries, including JOINs, WHERE clauses,
        and other SQL features supported by SQLite.
        
        Args:
            sql: SQL query string
            params: Optional tuple of parameters for parameterized queries
            
        Returns:
            List of dictionaries, where each dictionary represents a row
            
        Example:
            # Simple query
            results = db.query("SELECT * FROM schemas")
            
            # Query with JOIN
            results = db.query('''
                SELECT * FROM schemas 
                INNER JOIN tables ON tables.schema_name = schemas.schema_name
            ''')
            
            # Parameterized query
            results = db.query(
                "SELECT * FROM tables WHERE schema_name = ?", 
                ('public',)
            )
        """
        self.connect()
        cursor = self.connection.cursor()
        
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
            
        # Convert Row objects to dictionaries
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
        
    def query_with_columns(self, sql: str, params: Tuple = None) -> Tuple[List[Dict[str, Any]], List[str]]:
        """
        Execute a SQL query and return results with column names.
        
        Args:
            sql: SQL query string
            params: Optional tuple of parameters for parameterized queries
            
        Returns:
            Tuple of (data, columns) where data is a list of dictionaries and
            columns is a list of column names in order
        """
        self.connect()
        cursor = self.connection.cursor()
        
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
            
        # Get column names from cursor description
        columns = [description[0] for description in cursor.description]
        
        # Convert Row objects to dictionaries
        rows = cursor.fetchall()
        data = [dict(row) for row in rows]
        
        return data, columns
        
    def clear_all_tables(self):
        """
        Delete all data from all TAP_SCHEMA tables.
        
        Warning: This deletes all metadata. Use with caution!
        """
        self.connect()
        cursor = self.connection.cursor()
        
        # Delete in order to respect foreign key constraints
        cursor.execute('DELETE FROM key_columns')
        cursor.execute('DELETE FROM keys')
        cursor.execute('DELETE FROM columns')
        cursor.execute('DELETE FROM tables')
        cursor.execute('DELETE FROM schemas')
        
        self.connection.commit()
        
    def drop_all_tables(self):
        """
        Drop all TAP_SCHEMA tables from the database.
        
        Warning: This completely removes the schema. Use with extreme caution!
        """
        self.connect()
        cursor = self.connection.cursor()
        
        cursor.execute('DROP TABLE IF EXISTS key_columns')
        cursor.execute('DROP TABLE IF EXISTS keys')
        cursor.execute('DROP TABLE IF EXISTS columns')
        cursor.execute('DROP TABLE IF EXISTS tables')
        cursor.execute('DROP TABLE IF EXISTS schemas')
        
        self.connection.commit()
        
    def get_table_count(self, table_name: str) -> int:
        """
        Get the number of rows in a table.
        
        Args:
            table_name: Name of the table (schemas, tables, columns, keys, key_columns)
            
        Returns:
            Number of rows in the table
        """
        self.connect()
        cursor = self.connection.cursor()
        cursor.execute(f'SELECT COUNT(*) FROM {table_name}')
        return cursor.fetchone()[0]
