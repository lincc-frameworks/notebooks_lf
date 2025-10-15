#!/usr/bin/env python3

"""
Using queryparser, convert ADQL to calls to LSDB.

The template for the target code is:
import lsdb
cat = lsdb.open_catalog(
    "https://lsdb.data/io/hats/gaia_dr3/gaia/",
    columns=["source_id", "ra", "dec", "phot_g_mean_mag", "phot_variable_flag"],
    search_filter=lsdb.ConeSearch(ra=270, dec=23, radius_arcsec=3600)
    )
q = cat.query(
    "phot_g_mean_mag < 10 and phot_variable_flag == 'VARIABLE'"
    )
return q.head(15)
"""

import argparse
import sys
from queryparser.adql.adqltranslator import ADQLQueryTranslator, FormatListener, SelectQueryListener
from antlr4 import ParseTreeWalker

class LSDBFormatListener(FormatListener):
    """
    Listens to parsing events from a known subset of ADQL, building up the data in these
    events into data for parameters to LSDB.  These include:

    - Tables to catalogs -> gaia_dr3.gaia -> "https://lsdb.data/io/hats/gaia_dr3/gaia/"
    - Columns to columns -> SELECT source_id, ra, dec -> columns=["source_id", "ra", "dec"]
    - CONTAINS(POINT(...), CIRCLE(...))  -> lsdb.ConeSearch(...)
    - Basic conditions (e.g. phot_g_mean_mag < 10) -> filters= or cat.query(...)
    - Limits (e.g. TOP 10) -> q.head(limit)
    """
    def __init__(self, parser, contexts, limit_contexts):
        super().__init__(parser, contexts, limit_contexts)
        self.entities = {
            'tables': [],
            'columns': [],
            'spatial_search': None,
            'conditions': [],
            'limits': None
        }

    def enterContains(self, ctx):
        children = ctx.children[1:]
        if not children:
            return

        arg_texts = []
        current_arg = []
        paren_level = 0
        for child in children:
            t = getattr(child, 'getText', lambda: str(child))()
            if t == '(':
                paren_level += 1
                continue
            if t == ')':
                paren_level -= 1
                continue
            if t == ',' and paren_level == 1:
                arg_texts.append(''.join(current_arg).strip())
                current_arg = []
            else:
                current_arg.append(t)
        if current_arg:
            arg_texts.append(''.join(current_arg).strip())

        # Store spatial search information
        self.entities['spatial_search'] = {
            'type': 'ConeSearch',
            'args': arg_texts
        }

    def enterFrom_clause(self, ctx):
        """Extract table names from the FROM clause."""
        # Check for unsupported constructs first
        from_text = ctx.getText().upper()
        unsupported_keywords = ['JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'CROSS', 'UNION']

        for keyword in unsupported_keywords:
            if keyword in from_text:
                raise NotImplementedError(f"Unsupported SQL construct '{keyword}' found in FROM clause. Only simple FROM table_name is supported.")

        # Extract table name from simple FROM clause
        # Look for the table name after FROM keyword
        children_text = [child.getText() for child in ctx.children if hasattr(child, 'getText')]

        # Find the table name (should be after FROM and before any other keywords)
        for i, text in enumerate(children_text):
            if text.upper() == 'FROM' and i + 1 < len(children_text):
                table_name = children_text[i + 1]
                # Clean up any trailing commas or whitespace
                table_name = table_name.strip(',').strip()
                if table_name and not table_name.upper() in ['WHERE', 'ORDER', 'GROUP', 'HAVING']:
                    self.entities['tables'].append(table_name)
                    break

        return super().enterFrom_clause(ctx)

    def get_entities(self):
        return self.entities

def parse_adql_entities(adql: str) -> dict:
    """
    Parse ADQL query and extract the five entities of interest:
    - tables: List of table names (simple FROM clauses only)
    - columns: List of column names
    - spatial_search: Spatial search parameters (e.g., ConeSearch)
    - conditions: List of filter conditions
    - limits: Row limit information

    Returns:
        dict: Dictionary containing the extracted entities

    Raises:
        NotImplementedError: If unsupported SQL constructs are found
    """
    try:
        translator = ADQLQueryTranslator(adql)
        walker = ParseTreeWalker()
        select_query_listener = SelectQueryListener()
        walker.walk(select_query_listener, translator.tree)

        my_listener = LSDBFormatListener(
            translator.parser,
            contexts={},
            limit_contexts=select_query_listener.limit_contexts
        )
        walker.walk(my_listener, translator.tree)

        return my_listener.get_entities()
    except NotImplementedError as e:
        raise NotImplementedError(f"ADQL parsing failed: {e}")
    except Exception as e:
        raise ValueError(f"Failed to parse ADQL query: {e}")


def format_lsdb_code(entities: dict) -> str:
    """
    Convert parsed ADQL entities to Python code that uses LSDB calls.

    Args:
        entities: Dictionary containing parsed ADQL entities with keys:
                 'tables', 'columns', 'spatial_search', 'conditions', 'limits'

    Returns:
        str: Python code string using LSDB calls
    """
    code = "import lsdb\n\n"

    code += "cat = lsdb.open_catalog(\n"

    # Convert table names to catalog URLs (basic mapping for now)
    assert entities['tables']
    # For now, use the first table and convert to URL format
    table = entities['tables'][0]
    # Convert table name like 'gaiadr3.gaia' to URL format
    if '.' in table:
        parts = table.split('.')
        catalog_url = f"https://lsdb.data/io/hats/{parts[0]}/{parts[1]}/"
    else:
        catalog_url = f"https://lsdb.data/io/hats/{table}/"
    code += f"    '{catalog_url}',\n"

    # For now, this is a basic implementation that handles spatial search
    # Additional logic would be needed to handle columns, conditions, and limits
    if entities.get('spatial_search'):
        spatial = entities['spatial_search']
        if spatial['type'] == 'ConeSearch':
            code += f"    search_filter=lsdb.ConeSearch({', '.join(spatial['args'])}),\n"
    code += "    )\n\n"

    return code


def adql_to_lsdb(adql: str) -> str:
    """
    Convert ADQL query to Python code that uses LSDB calls.

    This function combines parsing and formatting by calling the two separate functions.
    """
    entities = parse_adql_entities(adql)
    return format_lsdb_code(entities)


def main():
    parser = argparse.ArgumentParser(description='Convert ADQL query to LSDB Python code')
    parser.add_argument('input', nargs='?', type=argparse.FileType('r'), default=sys.stdin,
                       help='Input file containing ADQL query (default: stdin)')

    args = parser.parse_args()

    try:
        adql_query = args.input.read()
        result = adql_to_lsdb(adql_query)
        print(result)
    except NotImplementedError as e:
        print(f"Error: {e}", file=sys.stderr)
        raise SystemExit(1)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        raise SystemExit(1)
    except KeyboardInterrupt:
        print("\nOperation cancelled.", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
