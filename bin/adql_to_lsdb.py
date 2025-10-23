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
      - or, CONTAINS(POINT(...), POLYGON(...)) -> lsdb.PolygonSearch(...)
    - Basic conditions (e.g. phot_g_mean_mag < 10) -> filters= or cat.query(...)
    - Limits (e.g. TOP 10) -> q.head(limit)
    """

    def __init__(self, parser, contexts, limit_contexts):
        super().__init__(parser, contexts, limit_contexts)
        self.entities = {
            "tables": [],
            "columns": [],
            "spatial_search": None,
            "conditions": [],
            "limits": None,
            "order_by": [],
        }
        # Track parsing context
        self._in_contains = False
        self._current_point = None
        self._current_circle = None
        self._current_polygon = None
        # Track WHERE clause parsing
        self._in_where = False
        self._current_conditions = []

    def enterContains(self, ctx):
        """Enter a CONTAINS clause - set context flag."""
        self._in_contains = True
        self._current_point = None
        self._current_circle = None
        self._current_polygon = None

    def exitContains(self, ctx):
        """Exit CONTAINS clause - validate and store spatial search info."""
        if not self._in_contains:
            return

        # Validate that we have both POINT and CIRCLE, or POINT and POLYGON
        if not self._current_point:
            raise NotImplementedError("CONTAINS clause must include a POINT")
        if not self._current_circle and not self._current_polygon:
            raise NotImplementedError("CONTAINS clause must include a CIRCLE or POLYGON")

        # Store spatial search information
        if self._current_circle:
            self.entities["spatial_search"] = {
                "type": "ConeSearch",
                "ra": self._current_point["ra"],
                "dec": self._current_point["dec"],
                "radius": self._current_circle["radius"],
            }
        elif self._current_polygon:
            self.entities["spatial_search"] = {
                "type": "PolygonSearch",
                "coordinates": self._current_polygon["coordinates"],
            }

        # Reset context
        self._in_contains = False
        self._current_point = None
        self._current_circle = None
        self._current_polygon = None

    def enterPoint(self, ctx):
        """Parse POINT('ICRS', ra, dec) within CONTAINS."""
        if not self._in_contains:
            return  # Ignore points outside CONTAINS

        # Extract arguments from the parsed context
        args = self._extract_function_args_from_context(ctx)
        assert args.pop(0).upper() == "POINT"

        if len(args) != 3:
            raise ValueError(f"POINT function expects 3 arguments, got {len(args)}")

        coord_system = args[0].strip("'\"")
        if coord_system.upper() != "ICRS":
            raise NotImplementedError(f"Only 'ICRS' coordinate system is supported, got '{coord_system}'")

        try:
            # These should match the RA, DEC columns from SELECT, but
            # we can ignore this sort of validation for now.
            pass
        except ValueError as e:
            raise ValueError(f"Invalid coordinates in POINT: {e}")

        # Again, these don't matter *yet* and would only matter for query validation.
        self._current_point = {"ra": "ra", "dec": "dec"}

    def enterCircle(self, ctx):
        """Parse CIRCLE('ICRS', ra, dec, radius) within CONTAINS."""
        if not self._in_contains:
            return  # Ignore circles outside CONTAINS

        # Extract arguments from the parsed context
        args = self._extract_function_args_from_context(ctx)
        assert args.pop(0).upper() == "CIRCLE"

        if len(args) != 4:
            raise ValueError(f"CIRCLE function expects 4 arguments, got {len(args)}")

        coord_system = args[0].strip("'\"")
        if coord_system.upper() != "ICRS":
            raise NotImplementedError(f"Only 'ICRS' coordinate system is supported, got '{coord_system}'")

        try:
            ra = float(args[1])
            dec = float(args[2])
            radius = float(args[3])
        except ValueError as e:
            raise ValueError(f"Invalid values in CIRCLE: {e}")

        self._current_circle = {"ra": ra, "dec": dec, "radius": radius}

    def enterPolygon(self, ctx):
        """Parse POLYGON('ICRS', x1, y1, x2, y2, ..., xn, yn) within CONTAINS."""
        if not self._in_contains:
            return  # Ignore polygons outside CONTAINS

        # Extract arguments from the parsed context
        args = self._extract_function_args_from_context(ctx)
        assert args.pop(0).upper() == "POLYGON"

        if len(args) < 4 or len(args) % 2 == 0:
            raise ValueError(f"POLYGON function expects an odd number of arguments >= 4, got {len(args)}")

        coord_system = args[0].strip("'\"")
        if coord_system.upper() != "ICRS":
            raise NotImplementedError(f"Only 'ICRS' coordinate system is supported, got '{coord_system}'")

        try:
            coordinates = []
            for i in range(1, len(args), 2):
                ra = float(args[i])
                dec = float(args[i + 1])
                coordinates.append((ra, dec))
        except ValueError as e:
            raise ValueError(f"Invalid values in POLYGON: {e}")

        self._current_polygon = {"coordinates": coordinates}

    def _extract_function_args_from_context(self, ctx):
        """Extract function arguments from ANTLR context by traversing the parse tree."""
        args = []

        # Walk through the children of the context
        for child in ctx.children:
            # Look for terminal nodes that represent the actual values
            if hasattr(child, "children"):
                # This is a non-terminal, recurse into it
                args.extend(self._extract_values_from_node(child))
            else:
                # Never mind punctuation like commas or parentheses
                if (text := str(child)) not in ("(", ")", ","):
                    args.append(text)

        # Merge unary signs with following numeric tokens (e.g., '-', '10.5' -> '-10.5')
        args = self._merge_unary_signs(args)
        return args

    def _extract_values_from_node(self, node):
        """Recursively extract values from a parse tree node."""
        values = []

        if hasattr(node, "children"):
            for child in node.children:
                values.extend(self._extract_values_from_node(child))
        else:
            # Terminal node
            # Never mind punctuation like commas or parentheses
            if (text := str(node)) not in ("(", ")", ","):
                values.append(text)

        return values

    def _merge_unary_signs(self, tokens):
        """
        Merge unary '+' or '-' tokens with immediately following numeric tokens,
        turning ['-', '10.5'] into ['-10.5'] so downstream parsing sees signed numbers.
        """
        merged = []
        i = 0
        while i < len(tokens):
            t = tokens[i]
            if t in ("-", "+") and i + 1 < len(tokens) and self._looks_like_number(tokens[i + 1]):
                merged.append(t + tokens[i + 1])
                i += 2
            else:
                merged.append(t)
                i += 1
        return merged

    def _looks_like_number(self, s: str) -> bool:
        try:
            float(s)
            return True
        except Exception:
            return False

    def enterSelect_list(self, ctx):
        """Parse the SELECT list to extract column names."""
        # Extract column names from the SELECT clause
        columns = self._extract_select_columns(ctx)
        self.entities["columns"].extend(columns)

    def _extract_select_columns(self, ctx):
        """Extract column names from a SELECT list context."""
        columns = []

        # Walk through the children to find column references
        for child in ctx.children:
            if hasattr(child, "children"):
                # This might be a select_sublist or derived_column
                column_name = self._extract_column_name(child)
                if column_name:
                    columns.append(column_name)

        return columns

    def _extract_column_name(self, node):
        """
        Extract a column name from a parse tree node.
        Handles simple column names and ignores complex expressions.
        """
        # For now, only handle simple column references
        # Get the text and clean it up
        text = node.getText().strip()

        # Skip if it's a comma or other punctuation
        if text in [",", "(", ")", "*"]:
            return None

        # Handle SELECT * case
        if text == "*":
            return "*"

        # For simple column names, just return the text
        # In a more complete implementation, we'd need to handle:
        # - table.column references
        # - aliased columns (column AS alias)
        # - function calls
        # - expressions

        # Skip SQL keywords that might appear
        if text.upper() in ["SELECT", "FROM", "WHERE", "TOP", "DISTINCT"]:
            return None

        return text

    def enterSet_limit(self, ctx):
        """Extract limit from SET LIMIT clause using pre-parsed limit_contexts."""
        # The SelectQueryListener has already extracted the limit information
        # limit_contexts is a dictionary where values contain the limit info
        if self.limit_contexts:
            for limit_text in self.limit_contexts.values():
                # Extract number specifically from LIMIT/TOP clause patterns
                import re

                # Match "LIMIT n" or "TOP n" patterns specifically
                match = re.search(r"(?:LIMIT|TOP)\s+(\d+)", limit_text, re.IGNORECASE)
                if match:
                    try:
                        limit_value = int(match.group(1))
                        if limit_value <= 0:
                            raise ValueError(f"TOP/LIMIT must be positive, got {limit_value}")
                        self.entities["limits"] = limit_value
                        return
                    except ValueError as e:
                        raise ValueError(f"Invalid TOP/LIMIT value: {e}")

    def enterWhere_clause(self, ctx):
        """Enter WHERE clause - start parsing conditions."""
        self._in_where = True
        self._current_conditions = []

    def exitWhere_clause(self, ctx):
        """Exit WHERE clause - finalize condition parsing."""
        if self._in_where:
            # Store conditions in simple format for AND-only cases
            if self._current_conditions:
                # For simple AND conditions, store as single list
                # When we add OR support, we'll use full DNF: [[cond1, cond2], [cond3, cond4]]
                self.entities["conditions"] = self._current_conditions

            # Reset context
            self._in_where = False
            self._current_conditions = []

    def enterComparison_predicate(self, ctx):
        """Parse comparison predicates like 'column < value'."""
        if not self._in_where:
            return  # Only process comparisons within WHERE clause

        # Skip CONTAINS comparisons - they're handled separately
        comparison_text = ctx.getText().upper()
        if "CONTAINS" in comparison_text:
            return

        # Extract the comparison components
        try:
            condition = self._parse_comparison(ctx)
            if condition:
                self._current_conditions.append(condition)
        except Exception as e:
            print(f"Warning: Could not parse comparison '{ctx.getText()}': {e}")

    def _parse_comparison(self, ctx):
        """
        Parse a comparison context into (column, operator, value) tuple.

        Examples:
        - 'phot_g_mean_mag < 10' -> ('phot_g_mean_mag', '<', 10)
        - "phot_variable_flag = 'VARIABLE'" -> ('phot_variable_flag', '==', 'VARIABLE')
        - 'dec >= -30' -> ('dec', '>=', -30)
        """
        # Get all tokens from the comparison
        tokens = []
        for child in ctx.children:
            if hasattr(child, "getText"):
                text = child.getText().strip()
                if text:
                    tokens.append(text)

        # Merge unary signs so we treat ['-', '30'] as ['-30']
        tokens = self._merge_unary_signs(tokens)

        # Look for basic pattern: column operator value
        if len(tokens) >= 3:
            # Find the operator (typically in the middle)
            sql_operators = ["<", ">", "<=", ">=", "=", "!=", "<>"]

            for i, token in enumerate(tokens):
                if token in sql_operators:
                    if i > 0 and i < len(tokens) - 1:
                        column = tokens[i - 1]
                        py_operator = self._translate_operator(token)
                        value = self._parse_value(tokens[i + 1])
                        return (column, py_operator, value)
        return None

    def _translate_operator(self, sql_operator):
        """Translate SQL operators to Python operators."""
        operator_map = {"=": "==", "<>": "!=", "!=": "!=", "<": "<", ">": ">", "<=": "<=", ">=": ">="}
        return operator_map.get(sql_operator, sql_operator)

    def _parse_value(self, value_text):
        """Parse a value, handling strings, numbers (incl. negative and scientific), etc."""
        # Remove quotes from string literals
        if value_text.startswith("'") and value_text.endswith("'"):
            return value_text[1:-1]  # Remove single quotes
        if value_text.startswith('"') and value_text.endswith('"'):
            return value_text[1:-1]  # Remove double quotes

        # Try to parse as number
        try:
            # Try integer first
            if "." not in value_text:
                return int(value_text)
            else:
                return float(value_text)
        except ValueError:
            # Return as string if not a number
            return value_text

    def enterFrom_clause(self, ctx):
        """Extract table names from the FROM clause."""
        # Check for unsupported constructs first
        from_text = ctx.getText().upper()
        unsupported_keywords = ["JOIN", "INNER", "LEFT", "RIGHT", "OUTER", "CROSS", "UNION"]

        for keyword in unsupported_keywords:
            if keyword in from_text:
                raise NotImplementedError(
                    f"Unsupported SQL construct '{keyword}' found in FROM clause. Only simple FROM table_name is supported."
                )

        # Extract table name from simple FROM clause
        # Look for the table name after FROM keyword
        children_text = [child.getText() for child in ctx.children if hasattr(child, "getText")]

        # Find the table name (should be after FROM and before any other keywords)
        for i, text in enumerate(children_text):
            if text.upper() == "FROM" and i + 1 < len(children_text):
                table_name = children_text[i + 1]
                # Clean up any trailing commas or whitespace
                table_name = table_name.strip(",").strip()
                if table_name and table_name.upper() not in ["WHERE", "ORDER", "GROUP", "HAVING"]:
                    self.entities["tables"].append(table_name)
                    break

        return super().enterFrom_clause(ctx)

    def _extract_sort_tokens(self, ctx):
        """Extract sort tokens from the ORDER BY clause context."""
        sort_tokens = []
        for child in ctx.children:
            if hasattr(child, "children"):
                # Non-terminal node
                spec = self._parse_sort_specification(child)
                if spec:
                    if isinstance(spec, list):
                        sort_tokens.extend(spec)
                    else:
                        sort_tokens.append(spec)
            else:
                # Terminal node
                text = child.getText().strip()
                if text and text not in (",", "ORDER", "BY"):
                    sort_tokens.append(text)
        return sort_tokens

    def _parse_sort_specification(self, node):
        """Parse individual sort specification from the parse tree node."""
        tokens = []
        for child in node.children:
            if hasattr(child, "children"):
                for grandchild in child.children:
                    if hasattr(grandchild, "getText"):
                        text = grandchild.getText().strip()
                        if text and text not in (",", "ORDER", "BY"):
                            tokens.append(text)
        return tokens

    def enterOrder_by_clause(self, ctx):
        """Parse ORDER BY clause into a list of (column, asc_bool) tuples."""
        order_by_list = self._extract_sort_tokens(ctx)

        # Arrange into (column, asc_bool) tuples
        order_by_tuples = []
        i = 0
        while i < len(order_by_list):
            col = order_by_list[i]
            asc = True  # Default to ascending
            if i + 1 < len(order_by_list):
                next_token = order_by_list[i + 1].upper()
                if next_token == "ASC":
                    asc = True
                    i += 1  # Skip next token
                elif next_token == "DESC":
                    asc = False
                    i += 1  # Skip next token
            order_by_tuples.append((col, asc))
            i += 1

        # Store into entities
        self.entities["order_by"] = order_by_tuples

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
            translator.parser, contexts={}, limit_contexts=select_query_listener.limit_contexts
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
    assert entities["tables"]
    # For now, use the first table and convert to URL format
    table = entities["tables"][0]
    # Convert table name like 'gaiadr3.gaia' to URL format
    if "." in table:
        parts = table.split(".")
        catalog_url = f"https://data.lsdb.io/hats/{parts[0]}/{parts[1]}/"
    else:
        catalog_url = f"https://data.lsdb.io/hats/{table}/"
    code += f"    '{catalog_url}',\n"
    if entities["columns"]:
        code += "    columns=[\n"
        code += "        " + ", ".join(f'"{col}"' for col in entities["columns"]) + "\n"
        code += "    ],\n"

    # Handle spatial search if present
    if entities.get("spatial_search"):
        spatial = entities["spatial_search"]
        if spatial["type"] == "ConeSearch":
            ra = spatial["ra"]
            dec = spatial["dec"]
            radius = spatial["radius"]
            code += f"    search_filter=lsdb.ConeSearch(ra={ra}, dec={dec}, radius_arcsec={radius * 3600}),\n"
        elif spatial["type"] == "PolygonSearch":
            coords = spatial["coordinates"]
            coord_list = ", ".join(f"({ra}, {dec})" for ra, dec in coords)
            code += f"    search_filter=lsdb.PolygonSearch([{coord_list}]),\n"

    # Apply conditions if present
    if entities.get("conditions"):
        conditions = entities["conditions"]
        code += f"    filters={conditions},\n"

    # Conclude open_catalog call
    code += "    )\n\n"

    # Handle limit if present
    if entities.get("limits"):
        limit_value = entities["limits"]
        code += f"result = cat.head({limit_value})\n"
    else:
        code += "result = cat.compute()\n"

    # Apply ORDER BY using pandas if requested
    order_by = entities.get("order_by") or []
    if order_by:
        cols = ", ".join(repr(col) for col, _ in order_by)
        asc_list = ", ".join("True" if asc else "False" for _, asc in order_by)
        # Use sort_values and reassign to result
        code += f"result = result.sort_values(by=[{cols}], ascending=[{asc_list}])\n"

    return code


def adql_to_lsdb(adql: str) -> str:
    """
    Convert ADQL query to Python code that uses LSDB calls.

    This function combines parsing and formatting by calling the two separate functions.
    """
    entities = parse_adql_entities(adql)
    return format_lsdb_code(entities)


def main():
    parser = argparse.ArgumentParser(description="Convert ADQL query to LSDB Python code")
    parser.add_argument(
        "input",
        nargs="?",
        type=argparse.FileType("r"),
        default=sys.stdin,
        help="Input file containing ADQL query (default: stdin)",
    )

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
