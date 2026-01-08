#!/usr/bin/env python3
"""
Sync Jira issues to Airtable.

This script fetches issues from Jira and syncs them to an Airtable table.
Improved version with better pagination, error handling, and logging.
"""

import os
import sys
import logging
import yaml
import requests
from requests.auth import HTTPBasicAuth
from pyairtable import Api
from typing import Dict, List, Any, Optional
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_credentials() -> Dict[str, str]:
    """Load credentials from creds.yaml or environment variables."""
    creds = {}

    # Load from creds.yaml (proper YAML format)
    if os.path.exists('creds.yaml'):
        try:
            with open('creds.yaml', 'r') as f:
                yaml_creds = yaml.safe_load(f)
                if yaml_creds and isinstance(yaml_creds, dict):
                    # Normalize keys to lowercase
                    creds = {k.lower(): v for k, v in yaml_creds.items()}
        except Exception as e:
            logger.warning(f"Could not read creds.yaml: {e}")

    # Environment variables override file (normalize to lowercase)
    env_keys = ['JIRA_SERVER', 'JIRA_EMAIL', 'JIRA_PAT', 'JIRA_PROJECT', 'JIRA_JQL',
                'AIRTABLE_PAT', 'AIRTABLE_BASE_ID', 'AIRTABLE_TABLE_NAME']
    for key in env_keys:
        if key in os.environ:
            creds[key.lower()] = os.environ[key]

    return creds


def test_jira_connection(jira_server: str, jira_email: str, jira_pat: str) -> bool:
    """
    Test connection to Jira API.

    Returns:
        True if connection successful, False otherwise
    """
    logger.info("Testing Jira connection...")

    url = f"{jira_server.rstrip('/')}/rest/api/3/myself"
    auth = HTTPBasicAuth(jira_email, jira_pat)
    headers = {"Accept": "application/json"}

    try:
        response = requests.get(url, headers=headers, auth=auth, timeout=10)
        response.raise_for_status()
        user_info = response.json()
        logger.info(f"✓ Successfully connected to Jira as: {user_info.get('displayName', jira_email)}")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"✗ Failed to connect to Jira: {e}")
        if hasattr(e, 'response') and e.response is not None:
            if e.response.status_code == 401:
                logger.error("Authentication failed. Please check your JIRA_EMAIL and JIRA_PAT")
            elif e.response.status_code == 403:
                logger.error("Access forbidden. Your account may not have API access")
            else:
                logger.error(f"Response: {e.response.text}")
        return False


def get_jira_issues(jira_server: str, jira_email: str, jira_pat: str,
                    jql: Optional[str] = None, project: Optional[str] = None,
                    max_results: int = 100) -> List[Dict[str, Any]]:
    """
    Fetch all issues from Jira with proper pagination.

    Args:
        jira_server: Jira server URL (e.g., https://your-company.atlassian.net)
        jira_email: Jira account email
        jira_pat: Jira API token
        jql: Optional JQL query to filter issues
        project: Optional project key to filter by
        max_results: Number of issues to fetch per request (default 100, max 100)

    Returns:
        List of issue dictionaries with key, summary, and url
    """
    logger.info(f"Fetching issues from Jira: {jira_server}")

    # Build JQL query
    if jql:
        query = jql
    elif project:
        query = f"project = {project} ORDER BY created DESC"
    else:
        query = "ORDER BY created DESC"

    logger.info(f"Using JQL: {query}")

    # Jira REST API endpoint - using /search/jql as /search is deprecated
    url = f"{jira_server.rstrip('/')}/rest/api/3/search/jql"

    auth = HTTPBasicAuth(jira_email, jira_pat)
    headers = {"Accept": "application/json"}

    all_issues = []
    max_results = min(max_results, 100)  # Jira Cloud limit is 100
    next_page_token = None
    page_num = 1

    while True:
        params = {
            "jql": query,
            "maxResults": max_results,
            "fields": "summary,issuetype,status,priority,assignee,created,updated,reporter,labels,components"
        }

        # Add nextPageToken for subsequent pages
        if next_page_token:
            params["nextPageToken"] = next_page_token

        try:
            logger.info(f"Fetching page {page_num} (up to {max_results} issues)...")
            response = requests.get(url, headers=headers, auth=auth, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            issues = data.get('issues', [])
            if not issues:
                logger.info("No more issues to fetch")
                break

            # Process issues
            for issue in issues:
                issue_key = issue['key']
                fields = issue['fields']

                # Handle labels and components (arrays)
                labels = fields.get('labels', [])
                components = fields.get('components', [])
                component_names = [c.get('name', '') for c in components] if components else []

                all_issues.append({
                    'key': issue_key,
                    'summary': fields.get('summary', ''),
                    'url': f"{jira_server.rstrip('/')}/browse/{issue_key}",
                    'type': fields.get('issuetype', {}).get('name', ''),
                    'status': fields.get('status', {}).get('name', ''),
                    'priority': fields.get('priority', {}).get('name', '') if fields.get('priority') else '',
                    'assignee': fields.get('assignee', {}).get('displayName', '') if fields.get('assignee') else '',
                    'reporter': fields.get('reporter', {}).get('displayName', '') if fields.get('reporter') else '',
                    'created': fields.get('created', ''),
                    'updated': fields.get('updated', ''),
                    'labels': ', '.join(labels) if labels else '',
                    'components': ', '.join(component_names) if component_names else ''
                })

            fetched_count = len(all_issues)
            logger.info(f"Fetched {len(issues)} issues (total so far: {fetched_count})")

            # Check if this is the last page
            is_last = data.get('isLast', True)
            if is_last:
                logger.info(f"✓ Successfully fetched all {fetched_count} issues")
                break

            # Get token for next page
            next_page_token = data.get('nextPageToken')
            if not next_page_token:
                logger.warning("No nextPageToken provided but isLast=false. Stopping pagination.")
                break

            page_num += 1

            # Add a small delay to avoid rate limiting
            time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching Jira issues: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response body: {e.response.text[:500]}")

                # Check for rate limiting
                if e.response.status_code == 429:
                    logger.warning("Rate limit hit. Returning issues fetched so far.")
                    return all_issues

            # Return what we have so far instead of failing completely
            if all_issues:
                logger.info(f"Returning {len(all_issues)} issues fetched before error")
                return all_issues
            raise

    logger.info(f"✓ Total issues fetched: {len(all_issues)}")
    return all_issues


def test_airtable_connection(api: Api, base_id: str, table_name: str) -> bool:
    """
    Test connection to Airtable.

    Returns:
        True if connection successful, False otherwise
    """
    logger.info("Testing Airtable connection...")

    try:
        table = api.table(base_id, table_name)
        # Try to fetch just one record to test connection
        table.first()
        logger.info(f"✓ Successfully connected to Airtable table: {table_name}")
        return True
    except Exception as e:
        logger.error(f"✗ Failed to connect to Airtable: {e}")
        logger.error("Please verify AIRTABLE_PAT, AIRTABLE_BASE_ID, and AIRTABLE_TABLE_NAME")
        return False


def sync_to_airtable(api: Api, base_id: str, table_name: str, issues: List[Dict[str, Any]]):
    """
    Sync Jira issues to Airtable.

    Args:
        api: Airtable API instance
        base_id: Airtable base ID
        table_name: Airtable table name
        issues: List of Jira issues
    """
    logger.info(f"Syncing {len(issues)} issues to Airtable table: {table_name}")

    table = api.table(base_id, table_name)

    # Get existing records
    logger.info("Fetching existing records from Airtable...")
    existing_records = table.all()
    existing_by_key = {rec['fields'].get('key'): rec for rec in existing_records}

    logger.info(f"Found {len(existing_records)} existing records in Airtable")

    created_count = 0
    updated_count = 0
    skipped_count = 0
    error_count = 0

    for i, issue in enumerate(issues, 1):
        try:
            issue_key = issue['key']

            # Prepare record data
            record_data = {
                'key': issue['key'],
                'summary': issue['summary'],
                'url': issue['url'],
                'type': issue.get('type', ''),
                'status': issue.get('status', ''),
                'priority': issue.get('priority', ''),
                'assignee': issue.get('assignee', ''),
                'reporter': issue.get('reporter', ''),
                'created': issue.get('created', ''),
                'updated': issue.get('updated', ''),
                'labels': issue.get('labels', ''),
                'components': issue.get('components', '')
            }

            if issue_key in existing_by_key:
                # Check if update is needed
                existing_record = existing_by_key[issue_key]
                existing_fields = existing_record['fields']

                needs_update = False
                for key, value in record_data.items():
                    if existing_fields.get(key) != value:
                        needs_update = True
                        break

                if needs_update:
                    table.update(existing_record['id'], record_data)
                    updated_count += 1
                    if updated_count % 10 == 0:
                        logger.info(f"Progress: {i}/{len(issues)} - Updated {updated_count} records...")
                else:
                    skipped_count += 1
            else:
                # Create new record
                table.create(record_data)
                created_count += 1
                if created_count % 10 == 0:
                    logger.info(f"Progress: {i}/{len(issues)} - Created {created_count} records...")

            # Add small delay every 10 records to avoid rate limiting
            if i % 10 == 0:
                time.sleep(0.2)

        except Exception as e:
            logger.error(f"Error syncing issue {issue.get('key', 'unknown')}: {e}")
            error_count += 1

    logger.info("=" * 60)
    logger.info("Sync Summary:")
    logger.info(f"  Created: {created_count}")
    logger.info(f"  Updated: {updated_count}")
    logger.info(f"  Skipped (unchanged): {skipped_count}")
    logger.info(f"  Errors: {error_count}")
    logger.info("=" * 60)


def main():
    """Main function."""
    # Load credentials
    try:
        creds = load_credentials()
    except Exception as e:
        logger.error(f"Failed to load credentials: {e}")
        sys.exit(1)

    # Get required credentials
    JIRA_SERVER = creds.get('jira_server')
    JIRA_EMAIL = creds.get('jira_email')
    JIRA_PAT = creds.get('jira_pat')
    AIRTABLE_BASE_ID = creds.get('airtable_base_id')
    AIRTABLE_TABLE_NAME = creds.get('airtable_table_name')
    AIRTABLE_PAT = creds.get('airtable_pat')

    # Optional filters
    JIRA_PROJECT = creds.get('jira_project')
    JIRA_JQL = creds.get('jira_jql')

    # Validate required variables
    missing_vars = []
    if not JIRA_SERVER:
        missing_vars.append('JIRA_SERVER')
    if not JIRA_EMAIL:
        missing_vars.append('JIRA_EMAIL')
    if not JIRA_PAT:
        missing_vars.append('JIRA_PAT')
    if not AIRTABLE_BASE_ID:
        missing_vars.append('AIRTABLE_BASE_ID')
    if not AIRTABLE_TABLE_NAME:
        missing_vars.append('AIRTABLE_TABLE_NAME')
    if not AIRTABLE_PAT:
        missing_vars.append('AIRTABLE_PAT')

    if missing_vars:
        logger.error("Missing required credentials:")
        for var in missing_vars:
            logger.error(f"  - {var}")
        logger.error("\nPlease set these in creds.yaml or as environment variables")
        sys.exit(1)

    # Test Jira connection
    if not test_jira_connection(JIRA_SERVER, JIRA_EMAIL, JIRA_PAT):
        logger.error("Cannot proceed without valid Jira connection")
        sys.exit(1)

    # Connect to Airtable
    try:
        api = Api(AIRTABLE_PAT)
        logger.info("✓ Airtable API initialized")
    except Exception as e:
        logger.error(f"Failed to initialize Airtable API: {e}")
        sys.exit(1)

    # Test Airtable connection
    if not test_airtable_connection(api, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME):
        logger.error("Cannot proceed without valid Airtable connection")
        sys.exit(1)

    # Fetch Jira issues
    try:
        issues = get_jira_issues(JIRA_SERVER, JIRA_EMAIL, JIRA_PAT,
                                 jql=JIRA_JQL, project=JIRA_PROJECT)
    except Exception as e:
        logger.error(f"Failed to fetch Jira issues: {e}")
        sys.exit(1)

    if not issues:
        logger.warning("No issues found matching the query")
        sys.exit(0)

    # Sync to Airtable
    try:
        sync_to_airtable(api, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME, issues)
        logger.info("✓ Sync completed successfully")
    except Exception as e:
        logger.error(f"Failed to sync to Airtable: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
