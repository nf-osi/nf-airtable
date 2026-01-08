#!/usr/bin/env python3
"""
Sync Jira issues to Airtable.

This script fetches issues from Jira and syncs them to an Airtable table.
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
    
    # Try to load from creds.yaml
    if os.path.exists('creds.yaml'):
        try:
            with open('creds.yaml', 'r') as f:
                content = f.read()
                
            # Try YAML format first
            if ':' in content and not content.strip().startswith('#'):
                try:
                    yaml_creds = yaml.safe_load(content)
                    if yaml_creds:
                        creds.update(yaml_creds)
                except yaml.YAMLError:
                    pass
            
            # Try key=value format
            if not creds:
                for line in content.splitlines():
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        creds[key.strip()] = value.strip().strip('"').strip("'")
        except Exception as e:
            logger.warning(f"Could not read creds.yaml: {e}")
    
    # Environment variables override file
    env_keys = ['JIRA_SERVER', 'JIRA_EMAIL', 'JIRA_PAT', 'JIRA_PROJECT', 'JIRA_JQL',
                'AIRTABLE_PAT', 'AIRTABLE_BASE_ID', 'AIRTABLE_TABLE_NAME']
    for key in env_keys:
        if key in os.environ:
            creds[key.lower()] = os.environ[key]
    
    return creds


def get_jira_issues(jira_server: str, jira_email: str, jira_pat: str, 
                    jql: Optional[str] = None, project: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Fetch issues from Jira.
    
    Args:
        jira_server: Jira server URL (e.g., https://your-company.atlassian.net)
        jira_email: Jira account email
        jira_pat: Jira API token
        jql: Optional JQL query to filter issues
        project: Optional project key to filter by
    
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
    
    # Jira REST API endpoint (using new /search/jql endpoint)
    url = f"{jira_server.rstrip('/')}/rest/api/3/search/jql"
    
    auth = HTTPBasicAuth(jira_email, jira_pat)
    headers = {
        "Accept": "application/json"
    }
    
    all_issues = []
    start_at = 0
    max_results = 100
    
    while True:
        params = {
            "jql": query,
            "startAt": start_at,
            "maxResults": max_results,
            "fields": "summary,issuetype,status,priority,assignee,created,updated"
        }
        
        try:
            response = requests.get(url, headers=headers, auth=auth, params=params)
            response.raise_for_status()
            data = response.json()
            
            issues = data.get('issues', [])
            if not issues:
                break
            
            for issue in issues:
                issue_key = issue['key']
                fields = issue['fields']
                
                all_issues.append({
                    'key': issue_key,
                    'summary': fields.get('summary', ''),
                    'url': f"{jira_server.rstrip('/')}/browse/{issue_key}",
                    'type': fields.get('issuetype', {}).get('name', ''),
                    'status': fields.get('status', {}).get('name', ''),
                    'priority': fields.get('priority', {}).get('name', '') if fields.get('priority') else '',
                    'assignee': fields.get('assignee', {}).get('displayName', '') if fields.get('assignee') else '',
                    'created': fields.get('created', ''),
                    'updated': fields.get('updated', '')
                })
            
            logger.info(f"Fetched {len(issues)} issues (total so far: {len(all_issues)})")
            
            # Continue fetching if we got a full page of results
            # (indicates there might be more)
            if len(issues) < max_results:
                # Got fewer than max_results, we've reached the end
                break
            
            start_at += max_results
            
            # Add a small delay to avoid rate limiting
            time.sleep(0.5)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching Jira issues: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text}")
                # Check if it's a rate limit error
                if e.response.status_code == 429:
                    logger.warning("Rate limit hit. Consider reducing sync frequency or limiting the number of issues.")
                    # Return what we have so far instead of failing completely
                    logger.info(f"Returning {len(all_issues)} issues fetched before rate limit")
                    return all_issues
            raise
    
    logger.info(f"Total issues fetched: {len(all_issues)}")
    return all_issues


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
    existing_records = table.all()
    existing_by_key = {rec['fields'].get('key'): rec for rec in existing_records}
    
    logger.info(f"Found {len(existing_records)} existing records in Airtable")
    
    created_count = 0
    updated_count = 0
    skipped_count = 0
    error_count = 0
    
    for issue in issues:
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
                'created': issue.get('created', ''),
                'updated': issue.get('updated', '')
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
                        logger.info(f"Updated {updated_count} records...")
                else:
                    skipped_count += 1
            else:
                # Create new record
                table.create(record_data)
                created_count += 1
                if created_count % 10 == 0:
                    logger.info(f"Created {created_count} records...")
                    
        except Exception as e:
            logger.error(f"Error syncing issue {issue.get('key', 'unknown')}: {e}")
            error_count += 1
    
    logger.info(
        f"Sync complete: {created_count} created, {updated_count} updated, "
        f"{skipped_count} skipped (unchanged), {error_count} errors"
    )


def main():
    """Main function."""
    # Load credentials
    try:
        creds = load_credentials()
    except Exception as e:
        logger.error(f"Failed to load credentials: {e}")
        sys.exit(1)
    
    # Get required environment variables (try both cases)
    JIRA_SERVER = os.environ.get('JIRA_SERVER', creds.get('jira_server') or creds.get('JIRA_SERVER'))
    JIRA_EMAIL = os.environ.get('JIRA_EMAIL', creds.get('jira_email') or creds.get('JIRA_EMAIL'))
    JIRA_PAT_RAW = os.environ.get('JIRA_PAT', creds.get('jira_pat') or creds.get('JIRA_PAT'))
    JIRA_PAT = JIRA_PAT_RAW[1:] if JIRA_PAT_RAW and JIRA_PAT_RAW.startswith(':') else JIRA_PAT_RAW
    AIRTABLE_BASE_ID = os.environ.get('AIRTABLE_BASE_ID', creds.get('airtable_base_id') or creds.get('AIRTABLE_BASE_ID'))
    AIRTABLE_TABLE_NAME = os.environ.get('AIRTABLE_TABLE_NAME', creds.get('airtable_table_name') or creds.get('AIRTABLE_TABLE_NAME'))
    AIRTABLE_PAT = creds.get('airtable_pat') or creds.get('AIRTABLE_PAT')
    
    # Optional filters
    JIRA_PROJECT = os.environ.get('JIRA_PROJECT', creds.get('jira_project') or creds.get('JIRA_PROJECT'))
    JIRA_JQL = os.environ.get('JIRA_JQL', creds.get('jira_jql') or creds.get('JIRA_JQL'))
    
    # Validate required variables
    if not JIRA_SERVER:
        logger.error("JIRA_SERVER environment variable is required")
        sys.exit(1)
    if not JIRA_EMAIL:
        logger.error("JIRA_EMAIL environment variable is required")
        sys.exit(1)
    if not JIRA_PAT:
        logger.error("JIRA_PAT not found in credentials file or environment")
        sys.exit(1)
    if not AIRTABLE_BASE_ID:
        logger.error("AIRTABLE_BASE_ID environment variable is required")
        sys.exit(1)
    if not AIRTABLE_TABLE_NAME:
        logger.error("AIRTABLE_TABLE_NAME environment variable is required")
        sys.exit(1)
    if not AIRTABLE_PAT:
        logger.error("AIRTABLE_PAT not found in credentials file or environment")
        sys.exit(1)
    
    # Connect to Airtable
    try:
        api = Api(AIRTABLE_PAT)
        logger.info("Successfully connected to Airtable")
    except Exception as e:
        logger.error(f"Failed to connect to Airtable: {e}")
        sys.exit(1)
    
    # Fetch Jira issues
    try:
        issues = get_jira_issues(JIRA_SERVER, JIRA_EMAIL, JIRA_PAT, 
                                 jql=JIRA_JQL, project=JIRA_PROJECT)
    except Exception as e:
        logger.error(f"Failed to fetch Jira issues: {e}")
        sys.exit(1)
    
    if not issues:
        logger.warning("No issues found")
        sys.exit(0)
    
    # Sync to Airtable
    try:
        sync_to_airtable(api, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME, issues)
        logger.info("Sync completed successfully")
    except Exception as e:
        logger.error(f"Failed to sync to Airtable: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

