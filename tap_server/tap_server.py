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
import adql_to_lsdb


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


def generate_sample_data(parsed_query):
    """
    Generate sample data based on the parsed query.
    
    Args:
        parsed_query: Dictionary with parsed query components
        
    Returns:
        Tuple of (data_list, columns_list)
    """
    columns = parsed_query.get('columns', ['*'])
    limit = parsed_query.get('limit', 10)
    
    # If columns is ['*'], use default columns
    if columns == ['*']:
        columns = ['ra', 'dec', 'mag', 'id']
    
    # Generate sample rows
    data = []
    for i in range(min(limit, 10)):  # Max 10 sample rows
        row = {}
        for col in columns:
            if col.lower() in ['ra', 'ra_deg']:
                row[col] = 180.0 + i * 0.1
            elif col.lower() in ['dec', 'dec_deg']:
                row[col] = -30.0 + i * 0.1
            elif col.lower() in ['mag', 'magnitude']:
                row[col] = 15.0 + i * 0.5
            elif col.lower() == 'id':
                row[col] = 1000 + i
            else:
                row[col] = i * 1.0
        data.append(row)
    
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
  -d "QUERY=SELECT ra, dec, mag FROM ztf_dr14 WHERE mag < 20 LIMIT 10"
        </pre>
        
        <h2>Supported ADQL Features</h2>
        <ul>
            <li>SELECT with column list or *</li>
            <li>FROM with table name</li>
            <li>WHERE clause (parsed but not executed)</li>
            <li>LIMIT clause</li>
        </ul>
        
        <p><em>Note: This is a prototype that returns sample data. Query execution is not yet implemented.</em></p>
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
        # Parse the ADQL query
        parsed = adql_to_lsdb.parse_adql(query)
        
        # Generate sample data
        data, columns = generate_sample_data(parsed)
        
        # Create query info
        query_info = {
            'query': query,
            'table': parsed.get('table', 'unknown')
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
