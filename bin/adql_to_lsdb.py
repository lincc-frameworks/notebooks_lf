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
        # Track parsing context
        self._in_contains = False
        self._current_point = None
        self._current_circle = None

    def enterContains(self, ctx):
        """Enter a CONTAINS clause - set context flag."""
        self._in_contains = True
        self._current_point = None
        self._current_circle = None

    def exitContains(self, ctx):
        """Exit CONTAINS clause - validate and store spatial search info."""
        if not self._in_contains:
            return

        # Validate that we have both POINT and CIRCLE
        if not self._current_point:
            raise NotImplementedError("CONTAINS clause must include a POINT")
        if not self._current_circle:
            raise NotImplementedError("CONTAINS clause must include a CIRCLE")

        # Validate that POINT coordinates match CIRCLE center
        if (self._current_point['ra'] != self._current_circle['ra'] or
            self._current_point['dec'] != self._current_circle['dec']):
            raise ValueError(
                f"POINT coordinates ({self._current_point['ra']}, {self._current_point['dec']}) "
                f"must match CIRCLE center ({self._current_circle['ra']}, {self._current_circle['dec']})"
            )

        # Store spatial search information
        self.entities['spatial_search'] = {
            'type': 'ConeSearch',
            'ra': self._current_point['ra'],
            'dec': self._current_point['dec'],
            'radius': self._current_circle['radius']
        }

        # Reset context
        self._in_contains = False
        self._current_point = None
        self._current_circle = None

    def enterPoint(self, ctx):
        """Parse POINT('ICRS', ra, dec) within CONTAINS."""
        if not self._in_contains:
            return  # Ignore points outside CONTAINS

        # Extract arguments from the parsed context
        args = self._extract_function_args_from_context(ctx)
        assert args.pop(0).upper() == 'POINT'

        if len(args) != 3:
            raise ValueError(f"POINT function expects 3 arguments, got {len(args)}")

        coord_system = args[0].strip("'\"")
        if coord_system.upper() != 'ICRS':
            raise NotImplementedError(f"Only 'ICRS' coordinate system is supported, got '{coord_system}'")

        try:
            ra = float(args[1])
            dec = float(args[2])
        except ValueError as e:
            raise ValueError(f"Invalid coordinates in POINT: {e}")

        self._current_point = {'ra': ra, 'dec': dec}

    def enterCircle(self, ctx):
        """Parse CIRCLE('ICRS', ra, dec, radius) within CONTAINS."""
        if not self._in_contains:
            return  # Ignore circles outside CONTAINS

        # Extract arguments from the parsed context
        args = self._extract_function_args_from_context(ctx)
        assert args.pop(0).upper() == 'CIRCLE'

        if len(args) != 4:
            raise ValueError(f"CIRCLE function expects 4 arguments, got {len(args)}")

        coord_system = args[0].strip("'\"")
        if coord_system.upper() != 'ICRS':
            raise NotImplementedError(f"Only 'ICRS' coordinate system is supported, got '{coord_system}'")

        try:
            ra = float(args[1])
            dec = float(args[2])
            radius = float(args[3])
        except ValueError as e:
            raise ValueError(f"Invalid values in CIRCLE: {e}")

        self._current_circle = {'ra': ra, 'dec': dec, 'radius': radius}

    def _extract_function_args_from_context(self, ctx):
        """Extract function arguments from ANTLR context by traversing the parse tree."""
        args = []

        # Walk through the children of the context
        for child in ctx.children:
            # Look for terminal nodes that represent the actual values
            if hasattr(child, 'children'):
                # This is a non-terminal, recurse into it
                args.extend(self._extract_values_from_node(child))
            else:
                # Never mind punctuation like commas or parentheses
                if (text := str(child)) not in ('(', ')', ','):
                    args.append(text)

        return args

    def _extract_values_from_node(self, node):
        """Recursively extract values from a parse tree node."""
        values = []

        if hasattr(node, 'children'):
            for child in node.children:
                values.extend(self._extract_values_from_node(child))
        else:
            # Terminal node
            # Never mind punctuation like commas or parentheses
            if (text := str(node)) not in ('(', ')', ','):
                values.append(text)

        return values

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

    # Handle spatial search if present
    if entities.get('spatial_search'):
        spatial = entities['spatial_search']
        if spatial['type'] == 'ConeSearch':
            ra = spatial['ra']
            dec = spatial['dec']
            radius = spatial['radius']
            code += f"    search_filter=lsdb.ConeSearch(ra={ra}, dec={dec}, radius_arcsec={radius * 3600}),\n"
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
