import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# PostgreSQL connection setup using environment variables
pg_conn_params = {
    'dbname': os.getenv('PG_DBNAME'),
    'user': os.getenv('PG_USER'),
    'password': os.getenv('PG_PASSWORD'),
    'host': os.getenv('PG_HOST'),
    'port': os.getenv('PG_PORT')
}

# Function to map Jira field types to PostgreSQL types
def map_jira_type_to_pg(jira_type):
    # The `field_type` is directly extracted from jira_fields table,
    # no need for predefined mapping unless you want to convert or standardize.
    return jira_type  # Return as it is

# Function to get field definitions from the jira_fields table
def get_jira_fields_from_pg():
    try:
        conn = psycopg2.connect(**pg_conn_params)
        cur = conn.cursor()

        # Query the jira_fields table to get field names and types
        cur.execute("SELECT field_name, field_type FROM jira_fields")
        fields = cur.fetchall()

        cur.close()
        conn.close()
        return fields

    except Exception as e:
        print(f"Error: {e}")
        return []

# Function to create a new table based on the Jira fields schema and handle duplicate names
def create_dynamic_table(fields, new_table_name):
    try:
        conn = psycopg2.connect(**pg_conn_params)
        cur = conn.cursor()

        # Drop the table if it already exists
        cur.execute(f"DROP TABLE IF EXISTS {new_table_name}")

        # Track duplicate field names and resolve by appending an index
        field_definitions = []
        field_name_count = {}

        for field_name, field_type in fields:
            # Handle duplicate field names by appending an index
            if field_name in field_name_count:
                field_name_count[field_name] += 1
                field_name = f"{field_name}_{field_name_count[field_name]}"
            else:
                field_name_count[field_name] = 0

            # Map the field_type as it is stored in the PostgreSQL table
            pg_field_type = map_jira_type_to_pg(field_type)
            field_definitions.append(f'"{field_name}" {pg_field_type}')

        # Build the CREATE TABLE SQL statement
        create_table_query = f"CREATE TABLE {new_table_name} ({', '.join(field_definitions)});"
        
        # Execute the table creation
        cur.execute(create_table_query)
        conn.commit()

        cur.close()
        conn.close()

        print(f"Table '{new_table_name}' created successfully with the appropriate field types.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    # Fetch field definitions from jira_fields table
    jira_fields = get_jira_fields_from_pg()

    if jira_fields:
        # Create a new table with a name of your choice
        new_table_name = 'dynamic_jira_table'  # Change this to your desired table name
        create_dynamic_table(jira_fields, new_table_name)
    else:
        print("No fields found in the jira_fields table.")