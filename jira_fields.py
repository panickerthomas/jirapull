from jira import JIRA
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Jira connection setup
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

# Retrieve Jira fields
def get_jira_fields():
    fields = jira.fields()
    result = []
    for field in fields:
        field_id = field['id']
        field_name = field['name']
        # Use .get() to safely access 'schema', and handle if it's missing
        field_schema = field.get('schema', {})
        field_type = field_schema.get('type', 'unknown')
        result.append((field_id, field_name, field_type))
    return result

# Map Jira field types to PostgreSQL types, with JSONB handling for complex types
def map_jira_type_to_pg(jira_type):
    type_mapping = {
        'string': 'VARCHAR',
        'number': 'NUMERIC',
        'array': 'TEXT[]',
        'date': 'DATE',
        'datetime': 'TIMESTAMP',
        'user': 'JSONB',     # Complex types such as user stored as JSONB
        'option': 'JSONB',   # Options can also be complex
        'any': 'JSONB',      # Catch-all for any unhandled type
        'unknown': 'TEXT'    # Default fallback
    }
    return type_mapping.get(jira_type, 'TEXT')

# Create PostgreSQL table based on Jira fields
def create_pg_table(fields):
    try:
        conn = psycopg2.connect(**pg_conn_params)
        cur = conn.cursor()

        # Drop table if exists
        cur.execute("DROP TABLE IF EXISTS jira_fields")

        # Create table SQL query
        create_table_query = """
        CREATE TABLE jira_fields (
            field_id VARCHAR PRIMARY KEY,
            field_name VARCHAR,
            field_type VARCHAR
        );
        """
        cur.execute(create_table_query)

        # Insert field data into the table
        for field_id, field_name, field_type in fields:
            insert_query = """
            INSERT INTO jira_fields (field_id, field_name, field_type) VALUES (%s, %s, %s)
            """
            cur.execute(insert_query, (field_id, field_name, map_jira_type_to_pg(field_type)))

        conn.commit()
        cur.close()
        conn.close()
        print("Table created and fields inserted successfully.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    jira_fields = get_jira_fields()
    create_pg_table(jira_fields)