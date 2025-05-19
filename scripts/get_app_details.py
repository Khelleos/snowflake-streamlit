import os
import sys
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def connect_to_snowflake():
    """Establish connection to Snowflake"""
    try:
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA'),
            role=os.getenv('SNOWFLAKE_ROLE')
        )
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {str(e)}")
        sys.exit(1)

def list_streamlit_apps(conn):
    """List all Streamlit apps in the account"""
    try:
        cursor = conn.cursor()
        
        # Get all Streamlit apps
        cursor.execute("""
            SHOW STREAMLITS
        """)
        
        # Get the column names
        columns = [desc[0] for desc in cursor.description]
        
        # Print apps in a formatted way
        print("\nStreamlit Apps:")
        print("=" * 80)
        
        for row in cursor.fetchall():
            print(f"\nApp Details:")
            print("-" * 40)
            for col, val in zip(columns, row):
                print(f"{col:20}: {val}")
            print("-" * 40)
        
        cursor.close()
        
    except Exception as e:
        print(f"Error listing Streamlit apps: {str(e)}")
        sys.exit(1)

def main():
    print("Listing all Streamlit apps...")
    conn = connect_to_snowflake()
    list_streamlit_apps(conn)
    conn.close()

if __name__ == "__main__":
    main() 