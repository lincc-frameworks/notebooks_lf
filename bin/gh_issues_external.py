#!/usr/bin/env python3

"""Visits all of the organization's repositories, collects open
issues, filters out all issues created by members of the organization,
and reports them either to text or HTML.

Supports fetching repositories from one organization and filtering
by members from a different organization using --org-repos and --org-members.

Depends on the GitHub CLI.
"""

import subprocess
import json
import argparse
from typing import Set, List, Dict
from datetime import datetime, timezone
import math

ORG_REPOS = "astronomy-commons"
ORG_MEMBERS = "astronomy-commons"


def get_org_members(org: str) -> Set[str]:
    print("Fetching org members...")
    result = subprocess.run(
        ["gh", "api", f"orgs/{org}/members", "--paginate", "--jq", ".[].login"],
        capture_output=True,
        text=True,
        check=True,
    )
    members = set(result.stdout.strip().splitlines())
    print(f"Found {len(members)} org members.")
    return members


def get_org_repos(org: str) -> List[str]:
    print("Fetching org repositories...")
    result = subprocess.run(
        ["gh", "api", f"orgs/{org}/repos", "--paginate", "--jq", ".[].name"],
        capture_output=True,
        text=True,
        check=True,
    )
    repos = result.stdout.strip().splitlines()
    print(f"Found {len(repos)} repositories.")
    return repos


def get_open_issues(org: str, repos: List[str]) -> List[Dict]:
    print("Fetching open issues for all repositories...")
    all_issues = []
    for repo in repos:
        print(f"  {repo}...")
        result = subprocess.run(
            [
                "gh",
                "issue",
                "list",
                "-R",
                f"{org}/{repo}",
                "--state",
                "open",
                "--json",
                "number,title,author,updatedAt,url",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0 and result.stdout.strip():
            try:
                issues = json.loads(result.stdout)
                all_issues.extend(issues)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON for repo {repo}: {e}")
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


def humanize_time(dt: datetime, now: datetime) -> str:
    delta = now - dt
    seconds = delta.total_seconds()
    if seconds < 60:
        return "just now"
    minutes = seconds / 60
    if minutes < 60:
        mins = math.floor(minutes)
        return f"{mins} minute{'s' if mins != 1 else ''} ago"
    hours = minutes / 60
    if hours < 24:
        hrs = math.floor(hours)
        return f"{hrs} hour{'s' if hrs != 1 else ''} ago"
    days = hours / 24
    if days < 30:
        dys = math.floor(days)
        return f"{dys} day{'s' if dys != 1 else ''} ago"
    months = days / 30
    if months < 12:
        mths = math.floor(months)
        return f"{mths} month{'s' if mths != 1 else ''} ago"
    yrs = math.floor(months / 12)
    return f"{yrs} year{'s' if yrs != 1 else ''} ago"


def get_humanized_updated_at(iso_time: str, now: datetime) -> str:
    try:
        dt = datetime.strptime(iso_time, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
        return humanize_time(dt, now)
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
n</body>
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
