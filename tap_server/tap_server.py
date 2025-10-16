"""
TAP (Table Access Protocol) Server Prototype

This is a prototype implementation of a TAP server following the IVOA TAP specification:
https://www.ivoa.net/documents/TAP/20181024/PR-TAP-1.1-20181024.html

The server accepts ADQL queries and returns results in VOTable format.
This prototype returns sample data instead of executing actual queries against a database.
"""

from flask import Flask, request, Response
import xml.etree.ElementTree as ET
from datetime import datetime
import sys
import os

import lsdb

# Add bin directory to path to import adql_to_lsdb
bin_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'bin')
sys.path.insert(0, bin_path)

from adql_to_lsdb import adql_to_lsdb, parse_adql_entities


app = Flask(__name__)


def create_votable_response(data, columns, query_info):
    """
    Create a VOTable XML response.

    Args:
        data: List of dictionaries containing row data
        columns: List of column names
        query_info: Dictionary with query metadata

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
        'value': datetime.utcnow().isoformat()
    })

    # Add TABLE element
    table = ET.SubElement(resource, 'TABLE', {'name': query_info.get('table', 'results')})

    # Add FIELD elements for each column
    for col in columns:
        field_attrs = {
            'name': col,
            'datatype': 'double',  # Simplified - in real implementation would detect type
            'unit': ''
        }
        # Handle special astronomical columns
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

    # Convert to string
    xml_str = ET.tostring(votable, encoding='unicode', method='xml')

    # Add XML declaration
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_str


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

    xml_str = ET.tostring(votable, encoding='unicode', method='xml')
    return '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_str


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
        # Convert ADQL query to LSDB code using bin/adql_to_lsdb
        entities = parse_adql_entities(query)

        # Convert table names to catalog URLs (basic mapping for now)
        assert entities['tables']
        # For now, use the first table and convert to URL format
        table = entities['tables'][0]
        # Convert table name like 'gaiadr3.gaia' to URL format
        # catalog_prefix = "https://data.lsdb.io/hats"
        catalog_prefix = "http://epyc.astro.washington.edu:43210/hats"
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
                filters=entities['conditions'],
                search_filter = lsdb.ConeSearch(
                    ra=spatial['ra'],
                    dec=spatial['dec'],
                    radius_arcsec=spatial['radius'] * 3600,
                    )

        cat = lsdb.open_catalog(
            catalog_url,
            columns=entities['columns'],
            search_filter=search_filter,
            )
        result_df = cat.head(entities['limits'])

        # Convert DataFrame to VOTable data format
        data, columns = dataframe_to_votable_data(result_df)

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

        # Create query info
        query_info = {
            'query': query,
            'table': table_name
        }

        # Generate response based on format
        if output_format in ['votable', 'votable/td']:
            xml_response = create_votable_response(data, columns, query_info)
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
        </language>
        <outputFormat>
            <mime>application/xml</mime>
            <alias>votable</alias>
        </outputFormat>
    </capability>
</capabilities>
"""
    return Response(xml, mimetype='application/xml')


@app.route('/tables')
def tables():
    """Return available tables metadata."""
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<tableset xmlns="http://www.ivoa.net/xml/VODataService/v1.1">
    <schema>
        <name>tap_schema</name>
        <description>Sample astronomical catalogs</description>
        <table>
            <name>ztf_dr14</name>
            <description>ZTF Data Release 14 (sample)</description>
            <column>
                <name>id</name>
                <dataType>long</dataType>
                <description>Object ID</description>
            </column>
            <column>
                <name>ra</name>
                <dataType>double</dataType>
                <unit>deg</unit>
                <ucd>pos.eq.ra;meta.main</ucd>
                <description>Right Ascension</description>
            </column>
            <column>
                <name>dec</name>
                <dataType>double</dataType>
                <unit>deg</unit>
                <ucd>pos.eq.dec;meta.main</ucd>
                <description>Declination</description>
            </column>
            <column>
                <name>mag</name>
                <dataType>double</dataType>
                <unit>mag</unit>
                <ucd>phot.mag</ucd>
                <description>Magnitude</description>
            </column>
        </table>
    </schema>
</tableset>
"""
    return Response(xml, mimetype='application/xml')


if __name__ == '__main__':
    print("Starting TAP Server Prototype...")
    print("Server will be available at http://localhost:5000")
    print("Press Ctrl+C to stop the server")
    app.run(host='0.0.0.0', port=5000, debug=True)
