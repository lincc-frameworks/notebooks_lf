# bin Directory

This directory contains scripts to be run by LSDB maintainers for managing and monitoring GitHub issues across the organization.

## Prerequisites

Before running the scripts in this directory, ensure you have the following:

1. **GitHub Personal Access Token**: A token with appropriate permissions to read organization data and issues.
   - Create a token at: https://github.com/settings/tokens
   - Required scopes: `read:org`, `repo` (or `public_repo` for public repositories only)
   - Set the token as an environment variable:
     ```bash
     export GITHUB_TOKEN="your_personal_access_token_here"
     ```

2. **Python packages**: Install the required Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```
   This will install the `human_readable` and `requests` packages, which provide human-readable formatting for dates and times, and HTTP request functionality.

## Scripts

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
