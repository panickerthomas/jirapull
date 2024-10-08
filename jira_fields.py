import os
import json
import psycopg2
from jira import JIRA
from dotenv import load_dotenv
from tqdm import tqdm  # Progress bar library

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
            batch = jira.search_issues(jql_query, startAt=start_at, maxResults=batch_size)
            issues.extend(batch)
            total_issues = batch.total  # Total number of issues based on the first response
            start_at += len(batch)
            pbar.update(len(batch))  # Update progress bar
    return issues

# Function to transform field names: replace spaces with underscores and prepend 'mss_'
def transform_field_name(field_name):
    return f"mss_{field_name.lower().replace(' ', '_')}"

# Recursive function to flatten and insert JSON data
def flatten_json_and_insert(issue_key, data, parent_key='', counter=0):
    """
    Recursively flatten a nested JSON structure and insert each key-value pair into the PostgreSQL table.
    """
    conn = psycopg2.connect(**pg_conn_params)
    cur = conn.cursor()

    if isinstance(data, dict):
        for key, value in data.items():
            new_key = f"{parent_key}_{key}" if parent_key else key  # Create new key
            flatten_json_and_insert(issue_key, value, new_key)
    elif isinstance(data, list):
        for i, item in enumerate(data):
            new_key = f"{parent_key}_{i}"  # Add the index of the list as a suffix to ensure uniqueness
            flatten_json_and_insert(issue_key, item, new_key)
    else:
        # Add a counter suffix to field_id if it already exists to avoid duplicate key errors
        insert_query = """
            INSERT INTO jira_fields_2 (issue_key, field_id, field_name, field_value)
            VALUES (%s, %s, %s, %s)
        """
        try:
            cur.execute(insert_query, (issue_key, parent_key, transform_field_name(parent_key), json.dumps(data)))
        except psycopg2.errors.UniqueViolation:
            # If there's a duplicate key error, append a unique counter to the key
            new_key_with_counter = f"{parent_key}_{counter}"
            cur.execute(insert_query, (issue_key, new_key_with_counter, transform_field_name(parent_key), json.dumps(data)))
            counter += 1

    conn.commit()
    cur.close()
    conn.close()

# Function to create a PostgreSQL table 'jira_fields_2'
def create_jira_fields_table():
    try:
        conn = psycopg2.connect(**pg_conn_params)
        cur = conn.cursor()

        # Drop table if it exists
        print("Dropping existing table if it exists...")
        cur.execute("DROP TABLE IF EXISTS jira_fields_2")
        conn.commit()

        # Build a SQL query to create the table based on the field names and types
        print("Creating new table 'jira_fields_2'...")
        create_table_query = """
            CREATE TABLE jira_fields_2 (
                issue_key TEXT,
                field_id TEXT,
                field_name TEXT,
                field_value JSONB,
                PRIMARY KEY (issue_key, field_id)
            )
        """
        cur.execute(create_table_query)
        conn.commit()

        print("Table 'jira_fields_2' created successfully.")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error creating 'jira_fields_2' table: {e}")

if __name__ == '__main__':
    # Fetch all issues
    issues = fetch_all_issues()

    if issues:
        # Create the PostgreSQL table
        create_jira_fields_table()

        # Loop through each issue and flatten its fields for insertion
        for issue in tqdm(issues, desc="Processing Jira Issues", unit="issue"):  # Progress bar added
            issue_key = issue.key
            issue_fields = issue.raw['fields']
            
            # Flatten the issue fields and insert them into the table
            flatten_json_and_insert(issue_key, issue_fields, parent_key='', counter=0)
    else:
        print("No issues to insert.")