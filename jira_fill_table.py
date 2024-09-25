import json
from jira import JIRA
import psycopg2
import os
from dotenv import load_dotenv
from tqdm import tqdm

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

# List of specific projects to process
allowed_projects = ['TO', 'CCMP', 'CLIP', 'CREMA', 'INFRA', 'ISD', 'MSSCI']

# Connect to Jira
jira = JIRA(server=jira_url, basic_auth=(jira_username, jira_token))

# Function to get field_id to field_name mappings from the jira_fields table
def get_jira_fields_mapping():
    try:
        conn = psycopg2.connect(**pg_conn_params)
        cur = conn.cursor()

        # Query the jira_fields table to get field_id and field_name mappings
        cur.execute("SELECT field_id, field_name FROM jira_fields")
        field_mappings = dict(cur.fetchall())

        cur.close()
        conn.close()
        return field_mappings

    except Exception as e:
        print(f"Error fetching Jira fields: {e}")
        return {}

# Function to handle complex fields (like dictionaries and arrays) and prepare them for PostgreSQL
def handle_complex_field(field_name, value):
    if isinstance(value, dict):
        # Convert dict to JSON for storing in a JSONB column
        return json.dumps(value)
    elif isinstance(value, list):
        if len(value) == 0:
            # Handle empty arrays: PostgreSQL requires '{}' for empty arrays
            return '{}'
        elif isinstance(value[0], dict):
            # If the list contains dicts, convert each dict to a JSON string
            return json.dumps([json.dumps(item) for item in value])
        else:
            # For other array types, return as is
            return value
    else:
        # Return the value as it is for non-complex types
        return value

# Function to properly quote column names
def quote_column_name(column_name):
    return f'"{column_name}"'

# Function to insert data into dynamic_jira_table
def insert_into_dynamic_jira_table(issue_data):
    try:
        conn = psycopg2.connect(**pg_conn_params)
        cur = conn.cursor()

        # Quote column names that may have special characters or spaces
        columns = ', '.join([quote_column_name(col) for col in issue_data.keys()])
        values = ', '.join(['%s'] * len(issue_data))
        insert_query = f"INSERT INTO dynamic_jira_table ({columns}) VALUES ({values})"

        # Adapt issue data to handle complex types (convert to JSONB or handle arrays)
        adapted_issue_data = [handle_complex_field(field_name, value) for field_name, value in issue_data.items()]

        cur.execute(insert_query, adapted_issue_data)
        conn.commit()

        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error inserting data into dynamic_jira_table: {e}")

# Function to get issues from a specific project
def get_issues_from_project(project_key):
    try:
        # Fetch up to 1000 issues from the specified project
        return jira.search_issues(f'project = {project_key}', maxResults=1000)
    except Exception as e:
        print(f"Error fetching issues for project '{project_key}': {e}")
        return []

# Function to get all issues from allowed projects and map field_ids to field_names
def get_issues_from_allowed_projects():
    # Get the field mappings from the jira_fields table
    field_mappings = get_jira_fields_mapping()

    # Fetch all projects from Jira
    projects = jira.projects()
    
    # Filter projects to only those in the allowed list
    filtered_projects = [project for project in projects if project.key in allowed_projects]
    total_projects = len(filtered_projects)

    print("Processing the following projects:")
    for project in filtered_projects:
        print(f"Project Name: {project.name}, Project Key: {project.key}")

    # Initialize tqdm progress bar for projects
    with tqdm(total=total_projects, desc="Processing Jira Projects") as project_pbar:
        for project in filtered_projects:
            project_key = project.key
            issues = get_issues_from_project(project_key)
            
            if issues:
                # Initialize tqdm progress bar for issues within the project
                with tqdm(total=len(issues), desc=f"Processing issues for project {project_key}") as issue_pbar:
                    for issue in issues:
                        issue_data = {}

                        # Map each field_id to field_name and prepare data for insertion
                        for field_id, value in issue.raw['fields'].items():
                            if field_id in field_mappings:
                                field_name = field_mappings[field_id]
                                issue_data[field_name] = value

                        # Insert mapped data into the dynamic_jira_table
                        if issue_data:
                            insert_into_dynamic_jira_table(issue_data)

                        # Update the progress bar for issues
                        issue_pbar.update(1)
            
            # Update the progress bar for projects
            project_pbar.update(1)

if __name__ == '__main__':
    get_issues_from_allowed_projects()