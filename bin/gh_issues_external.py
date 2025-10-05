#!/usr/bin/env python3

"""Visits all of the organization's repositories, collects open
issues, filters out all issues created by members of the organization,
and reports them either to text or HTML.

Supports fetching repositories from one organization and filtering
by members from a different organization using --org-repos and --org-members.

Requires a GitHub personal access token set in the GITHUB_TOKEN environment variable.
"""

import os
import json
import argparse
import re
from typing import Set, List, Dict, Optional
from datetime import datetime, timezone
import human_readable
import requests

ORG_REPOS = "astronomy-commons"
ORG_MEMBERS = "astronomy-commons"
GITHUB_API_BASE = "https://api.github.com"


def get_github_token() -> str:
    """Get GitHub token from environment variable."""
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        raise ValueError(
            "GITHUB_TOKEN environment variable is not set. "
            "Please set it to your GitHub personal access token."
        )
    return token


def create_github_session() -> requests.Session:
    """Create a requests session with GitHub authentication."""
    session = requests.Session()
    token = get_github_token()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28"
    })
    return session


def paginate_github_api(session: requests.Session, url: str) -> List[Dict]:
    """Paginate through GitHub API responses.
    
    Follows the Link header for pagination as documented in:
    https://docs.github.com/en/rest/guides/using-pagination-in-the-rest-api
    """
    results = []
    while url:
        response = session.get(url)
        response.raise_for_status()
        data = response.json()
        
        # Handle both list and dict responses
        if isinstance(data, list):
            results.extend(data)
        else:
            results.append(data)
        
        # Check for pagination using Link header (RFC 8288)
        # Format: <https://api.github.com/...?page=2>; rel="next", <...>; rel="last"
        link_header = response.headers.get("Link", "")
        url = None
        if link_header:
            # Match URLs within angle brackets that have rel="next"
            # Pattern: <URL>; rel="next"
            match = re.search(r'<([^>]+)>;\s*rel="next"', link_header)
            if match:
                url = match.group(1)
    
    return results


def get_org_members(org: str) -> Set[str]:
    print("Fetching org members...")
    session = create_github_session()
    url = f"{GITHUB_API_BASE}/orgs/{org}/members?per_page=100"
    members_data = paginate_github_api(session, url)
    members = {member["login"] for member in members_data}
    print(f"Found {len(members)} org members.")
    return members


def get_org_repos(org: str) -> List[str]:
    print("Fetching org repositories...")
    session = create_github_session()
    url = f"{GITHUB_API_BASE}/orgs/{org}/repos?per_page=100"
    repos_data = paginate_github_api(session, url)
    repos = [repo["name"] for repo in repos_data]
    print(f"Found {len(repos)} repositories.")
    return repos


def get_open_issues(org: str, repos: List[str]) -> List[Dict]:
    print("Fetching open issues for all repositories...")
    session = create_github_session()
    all_issues = []
    for repo in repos:
        print(f"  {repo}...")
        try:
            url = f"{GITHUB_API_BASE}/repos/{org}/{repo}/issues?state=open&per_page=100"
            issues_data = paginate_github_api(session, url)
            
            # Filter out pull requests (they also come through the issues API)
            issues = [
                {
                    "number": issue["number"],
                    "title": issue["title"],
                    "author": issue["user"],
                    "updatedAt": issue["updated_at"],
                    "url": issue["html_url"]
                }
                for issue in issues_data
                if "pull_request" not in issue
            ]
            all_issues.extend(issues)
        except requests.exceptions.RequestException as e:
            print(f"Error fetching issues for repo {repo}: {e}")
    print(f"Collected {len(all_issues)} open issues.")
    return all_issues


def filter_external_issues(issues: List[Dict], org_members: Set[str]) -> List[Dict]:
    print("Filtering issues by external authors...")
    external_issues = [
        issue
        for issue in issues
        if issue["author"] and issue["author"]["login"] not in org_members
    ]
    print(f"Found {len(external_issues)} external issues.")
    return external_issues


def get_humanized_updated_at(iso_time: str, now: datetime) -> str:
    try:
        dt = datetime.strptime(iso_time, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
        return human_readable.date_time(dt, when=now)
    except Exception:
        return iso_time


def print_text_issues(external_issues: List[Dict]):
    print("\nExternal Issues (most recent activity first):")
    now = datetime.now(timezone.utc)
    for issue in external_issues:
        author = issue["author"]["login"] if issue["author"] else "unknown"
        updated_human = get_humanized_updated_at(issue["updatedAt"], now)
        print(f"{updated_human} | {author} | {issue['title']} | {issue['url']}")


def write_html_issues(external_issues: List[Dict], html_file: str):
    print(f"Writing HTML output to {html_file} ...")
    html_header = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>External GitHub Issues</title>
<style>
body { font-family: sans-serif; margin: 2em; }
table { border-collapse: collapse; width: 100%; }
th, td { border: 1px solid #ccc; padding: 0.5em; text-align: left; }
th { background: #eee; }
tr:nth-child(even) { background: #f9f9f9; }
a { color: #0366d6; text-decoration: none; }
a:hover { text-decoration: underline; }
</style>
</head>
<body>
<h2>External Issues (most recent activity first)</h2>
<table>
<thead>
<tr>
<th>Last Activity</th>
<th>Author</th>
<th>Title</th>
<th>Issue Link</th>
</tr>
</thead>
<tbody>
"""
    now = datetime.now(timezone.utc)
    rows = []
    for issue in external_issues:
        author = issue["author"]["login"] if issue["author"] else "unknown"
        updated_human = get_humanized_updated_at(issue["updatedAt"], now)
        title = (
            issue["title"]
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
        url = issue["url"]
        rows.append(
            f"<tr>"
            f"<td>{updated_human}</td>"
            f"<td>{author}</td>"
            f"<td>{title}</td>"
            f'<td><a href="{url}">{url}</a></td>'
            f"</tr>"
        )
    html_footer = """
</tbody>
</table>
</body>
</html>
"""
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html_header)
        f.write("\n".join(rows))
        f.write(html_footer)
    print(f"HTML output written to {html_file}")


def main():
    parser = argparse.ArgumentParser(
        description="List open issues created by external users in a GitHub organization.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--org",
        type=str,
        help="GitHub organization (sets both --org-repos and --org-members)",
    )
    parser.add_argument(
        "--org-repos",
        type=str,
        default=ORG_REPOS,
        help="GitHub organization to fetch repositories and issues from",
    )
    parser.add_argument(
        "--org-members",
        type=str,
        default=ORG_MEMBERS,
        help="GitHub organization to fetch members from for filtering",
    )
    parser.add_argument(
        "--html",
        type=str,
        help="Output to HTML file.",
    )
    args = parser.parse_args()

    # If --org is provided, use it for both repos and members
    org_repos = args.org if args.org else args.org_repos
    org_members_arg = args.org if args.org else args.org_members

    org_members = get_org_members(org_members_arg)
    repos = get_org_repos(org_repos)
    all_issues = get_open_issues(org_repos, repos)
    external_issues = filter_external_issues(all_issues, org_members)
    # Sort by most recent activity
    external_issues.sort(key=lambda x: x["updatedAt"], reverse=True)

    if args.html:
        write_html_issues(external_issues, args.html)
    else:
        print_text_issues(external_issues)


if __name__ == "__main__":
    main()
