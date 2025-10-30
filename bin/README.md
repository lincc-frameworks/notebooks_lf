# bin Directory

This directory contains utility scripts for LSDB maintainers, including tools for managing GitHub issues and importing TAP schema metadata.

## Prerequisites

Before running the scripts in this directory, ensure you have the following:

1. **GitHub Personal Access Token**: A token with appropriate permissions to read organization data and issues.
   - Create a **classic** personal access token at: https://github.com/settings/tokens
   - **Important**: Use a "classic" token, not a "fine-grained" token (fine-grained tokens may not return all organization members)
   - Required scopes: `read:org`, `repo` (or `public_repo` for public repositories only)
   - Set the token as an environment variable:
     ```bash
     export GITHUB_TOKEN="your_personal_access_token_here"
     ```

2. **Python packages**: Install the required Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```
   This will install the required packages including `human_readable`, `requests`, `pyvo`, and other dependencies.

## Scripts

### import_tap_schema.py

This script imports TAP schema metadata from an external TAP server (such as IRSA or ESA Gaia) into a local SQLite database. It queries the remote TAP server for schema, table, and column metadata, then populates a local `tap_schema.db` file with this information.

#### Features
- Queries external TAP servers for schema metadata
- Supports filtering by schema/catalog name or specific table
- Creates and populates a local SQLite database with TAP schema structure
- Handles schemas, tables, and columns metadata
- Provides detailed logging and error reporting
- Uses IVOA TAP protocol standards

#### Usage

**Import a complete catalog/schema** (e.g., Gaia DR3 from IRSA):
```bash
./import_tap_schema.py --url https://irsa.ipac.caltech.edu/TAP --catalog gaiadr3
```

**Import from ESA Gaia TAP service**:
```bash
./import_tap_schema.py --url https://gea.esac.esa.int/tap-server/tap --schema gaiadr3
```

**Import a specific table**:
```bash
./import_tap_schema.py --url https://irsa.ipac.caltech.edu/TAP --table gaiadr3.gaia_source
```

**Use custom database path**:
```bash
./import_tap_schema.py --url https://irsa.ipac.caltech.edu/TAP --catalog gaiadr3 --db-path /path/to/tap_schema.db
```

**Enable verbose logging**:
```bash
./import_tap_schema.py --url https://irsa.ipac.caltech.edu/TAP --catalog gaiadr3 --verbose
```

#### Command-line Options

- `--url`: TAP service URL (required)
- `--catalog` or `--schema`: Catalog/schema name to import
- `--table`: Specific table name to import (can include schema prefix, e.g., `schema.table`)
- `--db-path`: Path to SQLite database file (default: `tap_schema.db` in current directory)
- `--verbose` or `-v`: Enable verbose logging

#### Database Structure

The script creates a SQLite database with the following tables:

- `schemas`: Schema metadata (schema_name, schema_description)
- `tables`: Table metadata (table_name, schema_name, table_description)
- `columns`: Column metadata (table_name, column_name, ucd, unit, datatype, description)

#### Example Workflow

1. Install dependencies: `pip install -r requirements.txt`
2. Run import: `./import_tap_schema.py --url https://irsa.ipac.caltech.edu/TAP --catalog gaiadr3`
3. Inspect the database: `sqlite3 tap_schema.db "SELECT * FROM tables;"`
4. Use the populated database with your TAP service implementation

### gh_issues_external.py

This script visits all of the organization's repositories, collects open issues, filters out all issues created by members of the organization, and reports them either to text or HTML format.

#### Features
- Fetches all repositories from a GitHub organization
- Collects all open issues from those repositories
- Filters out issues created by organization members
- Outputs results in text format (default) or HTML format
- Supports fetching repositories from one organization and filtering by members from a different organization

#### Usage

**Basic usage** (outputs to terminal):
```bash
./gh_issues_external.py --org-members lincc-frameworks
```

**Generate HTML report** (most common usage for maintainers):
```bash
./gh_issues_external.py --html external_issues.html --org-members lincc-frameworks
```

**Advanced usage** with separate organizations:
```bash
./gh_issues_external.py --org-repos astronomy-commons --org-members lincc-frameworks --html issues.html
```

**Use the same organization for both repos and members**:
```bash
./gh_issues_external.py --org lincc-frameworks --html external_issues.html
```

#### Command-line Options

- `--org`: GitHub organization (sets both `--org-repos` and `--org-members`)
- `--org-repos`: GitHub organization to fetch repositories and issues from (default: astronomy-commons)
- `--org-members`: GitHub organization to fetch members from for filtering (default: astronomy-commons)
- `--html`: Output to HTML file instead of text

#### Sample Run

Here's what most maintainers will want to run:
```bash
./gh_issues_external.py --html external_issues.html --org-members lincc-frameworks
```

This command will:
1. Fetch all members of the `lincc-frameworks` organization
2. Fetch all repositories from the default organization (`astronomy-commons`)
3. Collect all open issues from those repositories
4. Filter out issues created by `lincc-frameworks` members
5. Generate an HTML report saved as `external_issues.html`

The HTML report will contain a sortable table showing:
- Last activity timestamp (in human-readable format)
- Issue author
- Issue title
- Direct link to the issue
