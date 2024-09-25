import os
import json
import psycopg2
import logging
from jira import JIRA
from dotenv import load_dotenv
from tqdm import tqdm  # Progress bar library

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from .env file
load_dotenv()

# Jira API connection setup using environment variables
jira_url = os.getenv('JIRA_URL')
jira_username = os.getenv('JIRA_USERNAME')
jira_token = os.getenv('JIRA_TOKEN')

# PostgreSQL connection setup
pg_conn_params = {
    'dbname': os.getenv('PG_DBNAME'),
    'user': os.getenv('PG_USER'),
    'password': os.getenv('PG_PASSWORD'),
    'host': os.getenv('PG_HOST'),
    'port': os.getenv('PG_PORT')
}

# Connect to Jira
jira = JIRA(server=jira_url, basic_auth=(jira_username, jira_token))

# Function to fetch all issues with pagination
def fetch_all_issues(jql_query="project=MSSCI ORDER BY created DESC", batch_size=100):
    """
    Fetch all issues from Jira using pagination, handling large sets of issues.
    :param jql_query: The JQL query string to fetch issues.
    :param batch_size: Number of issues to retrieve per API request.
    :return: List of all Jira issues.
    """
    issues = []
    start_at = 0
    total_issues = 1  # Placeholder to enter the loop

    # Progress bar initialization
    with tqdm(desc="Fetching Jira Issues", unit="issues", leave=True) as pbar:
        while start_at < total_issues:
            try:
                batch = jira.search_issues(jql_query, startAt=start_at, maxResults=batch_size)
                issues.extend(batch)
                total_issues = batch.total  # Total number of issues based on the first response
                start_at += len(batch)
                pbar.update(len(batch))  # Update progress bar
            except Exception as e:
                logging.error(f"Error fetching issues: {e}")
                break
    return issues

# Function to transform field names: replace spaces with underscores and prepend 'mss_'
def transform_field_name(field_name):
    return f"mss_{field_name.lower().replace(' ', '_')}"

# Function to check if a field exists and if the value has changed
def check_and_update_field(cur, issue_key, field_id, new_field_value):
    """
    Check if the field exists and if the value has changed.
    If the value has changed, update the record. Otherwise, skip.
    """
    # Query to check if the field exists in the database
    check_query = """
        SELECT field_value FROM jira_fields_2
        WHERE issue_key = %s AND field_id = %s
    """
    cur.execute(check_query, (issue_key, field_id))
    result = cur.fetchone()

    # If field exists, check if the value has changed
    if result:
        current_value = result[0]
        if current_value != new_field_value:  # Compare old value with new value
            update_query = """
                UPDATE jira_fields_2
                SET field_value = %s
                WHERE issue_key = %s AND field_id = %s
            """
            cur.execute(update_query, (new_field_value, issue_key, field_id))
            logging.info(f"Updated {field_id} for issue {issue_key}")
    else:
        # Insert new record
        insert_query = """
            INSERT INTO jira_fields_2 (issue_key, field_id, field_name, field_value)
            VALUES (%s, %s, %s, %s)
        """
        cur.execute(insert_query, (issue_key, field_id, transform_field_name(field_id), new_field_value))

# Recursive function to flatten and insert/update JSON data
def flatten_json_and_insert(issue_key, data, parent_key='', cur=None):
    """
    Recursively flatten a nested JSON structure and insert/update each key-value pair into the PostgreSQL table.
    """
    if isinstance(data, dict):
        for key, value in data.items():
            new_key = f"{parent_key}_{key}" if parent_key else key  # Create new key
            flatten_json_and_insert(issue_key, value, new_key, cur)
    elif isinstance(data, list):
        for i, item in enumerate(data):
            new_key = f"{parent_key}_{i}"  # Add the index of the list as a suffix to ensure uniqueness
            flatten_json_and_insert(issue_key, item, new_key, cur)
    else:
        # Handle non-dict and non-list values by checking and updating
        new_field_value = json.dumps(data) if data is not None else None
        check_and_update_field(cur, issue_key, parent_key, new_field_value)

# Function to create a PostgreSQL table 'jira_fields_2' if it doesn't exist
def create_jira_fields_table():
    try:
        conn = psycopg2.connect(**pg_conn_params)
        cur = conn.cursor()

        # Create the table only if it doesn't exist
        logging.info("Ensuring the table 'jira_fields_2' exists...")
        create_table_query = """
            CREATE TABLE IF NOT EXISTS jira_fields_2 (
                issue_key TEXT,
                field_id TEXT,
                field_name TEXT,
                field_value JSONB,
                PRIMARY KEY (issue_key, field_id)
            )
        """
        cur.execute(create_table_query)
        conn.commit()

        logging.info("Table 'jira_fields_2' is ready for use.")

        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"Error ensuring 'jira_fields_2' table: {e}")

if __name__ == '__main__':
    # Fetch all issues
    issues = fetch_all_issues()

    if issues:
        # Create the PostgreSQL table if it doesn't exist
        create_jira_fields_table()

        # Open a database connection
        conn = psycopg2.connect(**pg_conn_params)
        cur = conn.cursor()

        try:
            # Loop through each issue and flatten its fields for insertion
            for i, issue in enumerate(tqdm(issues, desc="Processing Jira Issues", unit="issue")):
                issue_key = issue.key
                issue_fields = issue.raw['fields']
                
                # Flatten the issue fields and insert/update them into the table
                flatten_json_and_insert(issue_key, issue_fields, parent_key='', cur=cur)

                # Commit every 100 issues
                if i % 100 == 0:
                    conn.commit()

            # Final commit for any remaining issues
            conn.commit()
        except Exception as e:
            logging.error(f"Error processing issues: {e}")
        finally:
            cur.close()
            conn.close()
    else:
        logging.info("No issues to insert.")