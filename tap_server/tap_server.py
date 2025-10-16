"""
TAP (Table Access Protocol) Server Prototype

This is a prototype implementation of a TAP server following the IVOA TAP specification:
https://www.ivoa.net/documents/TAP/20181024/PR-TAP-1.1-20181024.html

The server accepts ADQL queries and returns results in VOTable format.
This prototype returns sample data instead of executing actual queries against a database.
"""

from flask import Flask, request, Response
import xml.etree.ElementTree as ET
from xml.dom import minidom
import datetime
import sys
import os

import lsdb

# Add bin directory to path to import adql_to_lsdb
bin_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'bin')
sys.path.insert(0, bin_path)

from adql_to_lsdb import adql_to_lsdb, parse_adql_entities

# Import TAP schema database module
from tap_schema_db import TAPSchemaDatabase


app = Flask(__name__)

# What is actually getting called?
@app.before_request
def log_request_info():
    print(f'Request URL: {request.url}')
    print(f'Request Method: {request.method}')
    print(f'Request Headers: {request.headers}')

# Initialize TAP schema database
# The database will be created if it doesn't exist when the server starts
TAP_SCHEMA_DB_PATH = os.path.join(os.path.dirname(__file__), 'tap_schema.db')
tap_schema_db = TAPSchemaDatabase(TAP_SCHEMA_DB_PATH, qualified='tap_schema')


def is_tap_schema_query(query_str: str):
    """Check if the query is for a TAP_SCHEMA table."""
    if not query_str:
        return False
    return 'tap_schema.' in query_str.lower()


def query_tap_schema(query_str: str):
    """
    Query TAP_SCHEMA metadata tables using SQLite.

    Returns:
        Tuple of (data, columns) where data is list of dicts and columns is list of column names
    """
    try:
        data, result_columns = tap_schema_db.query_with_columns(query_str, None)
        return data, result_columns
    except Exception as e:
        # If query fails, return empty result
        print(f"Error querying TAP_SCHEMA: {e}")
        return [], []


def get_column_metadata(table_name: str):
    """
    Get column metadata from tap_schema.db for a given table.

    Args:
        table_name: The table name to fetch metadata for (e.g., 'ztf_dr14' or 'public.ztf_dr14')

    Returns:
        Dictionary mapping column names to metadata dicts with keys: datatype, unit, ucd, description
    """
    if not table_name:
        return {}
    
    # Try the table name as-is first
    query = "SELECT column_name, datatype, unit, ucd, description FROM columns WHERE table_name = ?"
    try:
        tap_schema_db.connect()
        results = tap_schema_db.query(query, (table_name,))
        
        # If no results and table_name doesn't have schema prefix, try with 'public.' prefix
        if not results and '.' not in table_name:
            results = tap_schema_db.query(query, (f'public.{table_name}',))
        
        # Build metadata dictionary
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


def format_xml_with_indentation(element):
    """
    Format an XML element with proper indentation.
    
    Args:
        element: An ElementTree Element to format
        
    Returns:
        String containing formatted XML with proper indentation
    """
    # Convert to string
    xml_str = ET.tostring(element, encoding='unicode', method='xml')
    
    # Parse and format with minidom for indentation
    dom = minidom.parseString(xml_str)
    pretty_xml = dom.toprettyxml(indent="  ", encoding=None)
    
    # Remove the extra XML declaration that minidom adds (we'll add our own)
    lines = pretty_xml.split('\n')
    if lines[0].startswith('<?xml'):
        lines = lines[1:]
    
    # Remove empty lines at the end
    while lines and not lines[-1].strip():
        lines.pop()
    
    # Add XML declaration and return
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + '\n'.join(lines)


def create_votable_response(data, columns, query_info, column_metadata=None):
    """
    Create a VOTable XML response.

    Args:
        data: List of dictionaries containing row data
        columns: List of column names
        query_info: Dictionary with query metadata
        column_metadata: Optional dictionary mapping column names to metadata dicts
                        (with keys: datatype, unit, ucd, description)

    Returns:
        String containing VOTable XML
    """
    # Create VOTable root element
    votable = ET.Element('VOTABLE', {
        'version': '1.4',
        'xmlns': 'http://www.ivoa.net/xml/VOTable/v1.4',
        'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance'
    })

    # Add RESOURCE element
    resource = ET.SubElement(votable, 'RESOURCE', {'type': 'results'})

    # Add INFO elements for query metadata
    ET.SubElement(resource, 'INFO', {
        'name': 'QUERY_STATUS',
        'value': 'OK'
    })

    ET.SubElement(resource, 'INFO', {
        'name': 'QUERY',
        'value': query_info.get('query', '')
    })

    ET.SubElement(resource, 'INFO', {
        'name': 'TIMESTAMP',
        'value': datetime.datetime.now(datetime.UTC).isoformat()
    })

    # Add TABLE element
    table = ET.SubElement(resource, 'TABLE', {'name': query_info.get('table', 'results')})

    # Add FIELD elements for each column
    for col in columns:
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
            # Warn about missing metadata
            table_name = query_info.get('table', 'unknown')
            print(f"WARNING: Missing metadata for column '{col}' in table '{table_name}'. Using fallback values.")
            
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

        ET.SubElement(table, 'FIELD', field_attrs)

    # Add DATA element with TABLEDATA
    data_elem = ET.SubElement(table, 'DATA')
    tabledata = ET.SubElement(data_elem, 'TABLEDATA')

    # Add rows
    for row_data in data:
        tr = ET.SubElement(tabledata, 'TR')
        for col in columns:
            td = ET.SubElement(tr, 'TD')
            value = row_data.get(col, '')
            td.text = str(value) if value is not None else ''

    # Format and return the XML with proper indentation
    return format_xml_with_indentation(votable)


def create_error_votable(error_message, query=''):
    """
    Create a VOTable error response.

    Args:
        error_message: Error message string
        query: The original query

    Returns:
        String containing VOTable XML with error
    """
    votable = ET.Element('VOTABLE', {
        'version': '1.4',
        'xmlns': 'http://www.ivoa.net/xml/VOTable/v1.4'
    })

    resource = ET.SubElement(votable, 'RESOURCE', {'type': 'results'})

    ET.SubElement(resource, 'INFO', {
        'name': 'QUERY_STATUS',
        'value': 'ERROR'
    })

    ET.SubElement(resource, 'INFO', {
        'name': 'ERROR',
        'value': error_message
    })

    if query:
        ET.SubElement(resource, 'INFO', {
            'name': 'QUERY',
            'value': query
        })

    # Format and return the XML with proper indentation
    return format_xml_with_indentation(votable)


def dataframe_to_votable_data(df):
    """
    Convert a pandas DataFrame to VOTable data format.

    Args:
        df: pandas DataFrame

    Returns:
        Tuple of (data_list, columns_list) where data_list is a list of dicts
    """
    # Get column names
    columns = df.columns.tolist()

    # Convert DataFrame to list of dictionaries
    data = df.to_dict('records')

    return data, columns


@app.route('/')
def index():
    """Root endpoint with server information."""
    html = """
    <html>
    <head><title>TAP Server Prototype</title></head>
    <body>
        <h1>TAP Server Prototype</h1>
        <p>This is a prototype implementation of a TAP (Table Access Protocol) server.</p>

        <h2>Endpoints</h2>
        <ul>
            <li><strong>GET/POST /sync</strong> - Synchronous query endpoint</li>
            <li><strong>GET /capabilities</strong> - Service capabilities</li>
            <li><strong>GET /tables</strong> - Available tables</li>
        </ul>

        <h2>Example Query</h2>
        <p>Submit an ADQL query to the /sync endpoint:</p>
        <pre>
curl -X POST http://localhost:5000/sync \\
  -d "REQUEST=doQuery" \\
  -d "LANG=ADQL" \\
  -d "QUERY=SELECT TOP 10 ra, dec, mag FROM ztf_dr14 WHERE mag < 20"
        </pre>

        <h2>Supported ADQL Features</h2>
        <ul>
            <li>SELECT with column list or *</li>
            <li>FROM with table name</li>
            <li>WHERE clause with comparison operators</li>
            <li>CONTAINS with POINT and CIRCLE for cone searches</li>
            <li>TOP/LIMIT clause</li>
        </ul>

        <p><em>Note: This server uses the queryparser library to convert ADQL to LSDB operations. Query execution returns sample data for testing.</em></p>
    </body>
    </html>
    """
    return html


@app.route('/sync', methods=['GET', 'POST'])
def sync_query():
    """
    Synchronous query endpoint following TAP specification.

    Accepts parameters:
        REQUEST: Must be 'doQuery'
        LANG: Query language (ADQL)
        QUERY: The ADQL query string
        FORMAT: Output format (default: votable)
    """
    # Get parameters from either GET or POST
    if request.method == 'POST':
        params = request.form
    else:
        params = request.args

    # Validate REQUEST parameter
    request_type = params.get('REQUEST', '')
    if request_type != 'doQuery':
        error_msg = f"Invalid REQUEST parameter: {request_type}. Must be 'doQuery'."
        return Response(
            create_error_votable(error_msg),
            mimetype='application/xml',
            status=400
        )

    # Validate LANG parameter
    lang = params.get('LANG', 'ADQL')
    if lang.upper() != 'ADQL':
        error_msg = f"Unsupported query language: {lang}. Only ADQL is supported."
        return Response(
            create_error_votable(error_msg),
            mimetype='application/xml',
            status=400
        )

    # Get query
    query = params.get('QUERY', '')
    if not query:
        error_msg = "Missing required parameter: QUERY"
        return Response(
            create_error_votable(error_msg),
            mimetype='application/xml',
            status=400
        )

    # Get format (default to votable)
    output_format = params.get('FORMAT', 'votable').lower()

    try:
        # Check if this is a TAP_SCHEMA query
        if is_tap_schema_query(query):
            # Query the TAP_SCHEMA metadata
            data, result_columns = query_tap_schema(query)
            # NOTE: Not sure this is quite right.  What comes back from caltech TAP?
            table_name = 'tap_schema'
        else:
            # Parse the ADQL query to get entities
            entities = parse_adql_entities(query)

            # Get the table name from the query
            assert entities['tables']
            table = entities['tables'][0]

            # Handle regular catalog query
            # Convert table name like 'gaiadr3.gaia' to URL format
            # catalog_prefix = "https://data.lsdb.io/hats"
            # catalog_prefix = "http://epyc.astro.washington.edu:43210/hats"
            catalog_prefix = "/epyc/data3/hats/catalogs"
            if '.' in table:
                parts = table.split('.')
                catalog_url = f"{catalog_prefix}/{parts[0]}/{parts[1]}/"
            else:
                catalog_url = f"{catalog_prefix}/{table}/"

            search_filter = None
            if entities.get('spatial_search'):
                spatial = entities['spatial_search']
                if spatial['type'] == 'ConeSearch':
                    ra = spatial['ra']
                    dec = spatial['dec']
                    filters=entities['conditions']
                    print(filters)
                    search_filter = lsdb.ConeSearch(
                        ra=spatial['ra'],
                        dec=spatial['dec'],
                        radius_arcsec=spatial['radius'] * 3600,
                        )

            cat = lsdb.open_catalog(
                catalog_url,
                columns=entities['columns'],
                search_filter=search_filter,
                # NOTE: these filters sort of work, but fail with string values like "VARIABLE"
                filters=filters,
                )
            result_df = cat.head(entities['limits'])

            # Convert DataFrame to VOTable data format
            data, result_columns = dataframe_to_votable_data(result_df)

            # Extract table name from query for metadata using regex
            # Good examples that match: "FROM ztf_dr14", "FROM gaia_dr3.gaia", "from MyTable WHERE"
            # Bad examples that don't match (return 'results'): "FROMAGE", "FROM WHERE", "FROM LIMIT"
            table_name = 'results'
            import re

            # SQL keywords that should not be considered table names
            sql_keywords = {
                'WHERE', 'SELECT', 'ORDER', 'GROUP', 'HAVING', 'LIMIT', 'OFFSET',
                'UNION', 'INTERSECT', 'EXCEPT', 'JOIN', 'INNER', 'LEFT', 'RIGHT',
                'OUTER', 'CROSS', 'ON', 'AS', 'AND', 'OR', 'NOT', 'IN', 'EXISTS',
                'BETWEEN', 'LIKE', 'IS', 'NULL', 'DISTINCT', 'ALL', 'ANY', 'SOME'
            }

            # Match FROM keyword followed by table name (optionally schema.table)
            # Pattern: \bFROM\s+ matches FROM with word boundary followed by whitespace
            # ([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?) captures table or schema.table
            match = re.search(
                r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)\b',
                query, re.IGNORECASE)

            if match:
                candidate = match.group(1)
                # Validate the candidate is not a SQL keyword (check table part for schema.table)
                table_part = candidate.split('.')[-1]
                if table_part.upper() not in sql_keywords:
                    table_name = candidate

        # Get column metadata from tap_schema.db
        column_metadata = get_column_metadata(table_name)

        # Create query info
        query_info = {
            'query': query,
            'table': table_name
        }

        # Generate response based on format
        if output_format in ['votable', 'votable/td']:
            xml_response = create_votable_response(data, result_columns, query_info, column_metadata)
            return Response(xml_response, mimetype='application/xml')
        else:
            error_msg = f"Unsupported format: {output_format}"
            return Response(
                create_error_votable(error_msg, query),
                mimetype='application/xml',
                status=400
            )

    except Exception as e:
        error_msg = f"Error processing query: {str(e)}"
        return Response(
            create_error_votable(error_msg, query),
            mimetype='application/xml',
            status=500
        )


@app.route('/capabilities')
def capabilities():
    """Return service capabilities in VOSI format."""
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<capabilities xmlns="http://www.ivoa.net/xml/VOSICapabilities/v1.0"
              xmlns:vr="http://www.ivoa.net/xml/VOResource/v1.0"
              xmlns:tr="http://www.ivoa.net/xml/TAPRegExt/v1.0"
              xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <capability standardID="ivo://ivoa.net/std/TAP">
        <interface xsi:type="tr:ParamHTTP" role="std">
            <accessURL use="base">http://localhost:5000/</accessURL>
        </interface>
        <language>
            <name>ADQL</name>
            <version ivo-id="ivo://ivoa.net/std/ADQL#v2.0">2.0</version>
            <languageFeatures type="ivo://ivoa.net/std/TAPRegExt#features-adqlgeo">
                <feature>
                    <form>CIRCLE</form>
                </feature>
                <feature>
                    <form>POINT</form>
                </feature>
                <feature>
                    <form>CONTAINS</form>
                </feature>
                <feature>
                    <form>POLYGON</form>
                </feature>
            </languageFeatures>
        </language>
        <outputFormat>
            <mime>application/xml</mime>
            <alias>votable</alias>
        </outputFormat>
    </capability>
</capabilities>
"""
    return Response(xml, mimetype='application/xml')


def generate_tables_xml():
    """
    Generate tables metadata XML from tap_schema.db.
    
    Returns:
        String containing tableset XML with schemas, tables, and columns
    """
    # Create tableset root element
    tableset = ET.Element('tableset', {
        'xmlns': 'http://www.ivoa.net/xml/VODataService/v1.1'
    })
    
    try:
        # Query schemas from database
        tap_schema_db.connect()
        schemas = tap_schema_db.query("SELECT schema_name, description FROM schemas ORDER BY schema_name")
        
        for schema_row in schemas:
            schema_name = schema_row['schema_name']
            
            # Create schema element
            schema_elem = ET.SubElement(tableset, 'schema')
            name_elem = ET.SubElement(schema_elem, 'name')
            name_elem.text = schema_name
            
            if schema_row.get('description'):
                desc_elem = ET.SubElement(schema_elem, 'description')
                desc_elem.text = schema_row['description']
            
            # Query tables for this schema
            tables = tap_schema_db.query(
                "SELECT table_name, description FROM tables WHERE schema_name = ? ORDER BY table_name",
                (schema_name,)
            )
            
            for table_row in tables:
                table_name = table_row['table_name']
                
                # Create table element
                table_elem = ET.SubElement(schema_elem, 'table')
                table_name_elem = ET.SubElement(table_elem, 'name')
                table_name_elem.text = table_name
                
                if table_row.get('description'):
                    table_desc_elem = ET.SubElement(table_elem, 'description')
                    table_desc_elem.text = table_row['description']
                
                # Query columns for this table
                # Table names in columns table are fully qualified (schema.table)
                full_table_name = f"{schema_name}.{table_name}"
                columns = tap_schema_db.query(
                    "SELECT column_name, datatype, unit, ucd, description FROM columns WHERE table_name = ? ORDER BY column_name",
                    (full_table_name,)
                )
                
                for column_row in columns:
                    # Create column element
                    column_elem = ET.SubElement(table_elem, 'column')
                    
                    col_name_elem = ET.SubElement(column_elem, 'name')
                    col_name_elem.text = column_row['column_name']
                    
                    if column_row.get('datatype'):
                        datatype_elem = ET.SubElement(column_elem, 'dataType')
                        datatype_elem.text = column_row['datatype']
                    
                    if column_row.get('unit'):
                        unit_elem = ET.SubElement(column_elem, 'unit')
                        unit_elem.text = column_row['unit']
                    
                    if column_row.get('ucd'):
                        ucd_elem = ET.SubElement(column_elem, 'ucd')
                        ucd_elem.text = column_row['ucd']
                    
                    if column_row.get('description'):
                        col_desc_elem = ET.SubElement(column_elem, 'description')
                        col_desc_elem.text = column_row['description']
        
        # Format and return the XML with proper indentation
        return format_xml_with_indentation(tableset)
        
    except Exception as e:
        # Note: Using print() for consistency with existing error handling in this file
        print(f"ERROR: Failed to generate tables XML from tap_schema.db: {e}")
        # Return minimal valid XML on error
        return '<?xml version="1.0" encoding="UTF-8"?>\n<tableset xmlns="http://www.ivoa.net/xml/VODataService/v1.1"/>'


@app.route('/tables')
def tables():
    """Return available tables metadata from tap_schema.db."""
    xml = generate_tables_xml()
    return Response(xml, mimetype='application/xml')


if __name__ == '__main__':
    port = 43213
    print("Starting TAP Server Prototype...")
    print(f"Server will be available at http://localhost:{port}")
    print("Press Ctrl+C to stop the server")
    app.run(host='0.0.0.0', port=port, debug=True)
