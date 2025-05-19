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

def get_app_files(conn, app_name):
    """Get details and files for a specific Streamlit app"""
    try:
        cursor = conn.cursor()
        
        # First, let's see what columns are available
        cursor.execute("""
            SELECT column_name 
            FROM STREAMLIT_DB.INFORMATION_SCHEMA.COLUMNS 
            WHERE table_name = 'STREAMLITS'
        """)
        
        print("\nAvailable columns in STREAMLITS view:")
        print("=" * 80)
        columns = [row[0] for row in cursor.fetchall()]
        print(columns)
        
        # Now get app details
        cursor.execute(f"""
            SELECT *
            FROM STREAMLIT_DB.INFORMATION_SCHEMA.STREAMLITS
            WHERE STREAMLIT_NAME = '{app_name}'
        """)
        
        # Get the column names
        columns = [desc[0] for desc in cursor.description]
        app_details = cursor.fetchone()
        
        if not app_details:
            print(f"\nStreamlit app '{app_name}' not found")
            return
        
        # Create a dictionary of app details
        app_info = dict(zip(columns, app_details))
        
        # Print app details
        print("\nStreamlit App Details:")
        print("=" * 80)
        for col, val in app_info.items():
            print(f"{col:20}: {val}")
        
        # Get the root location from app details
        root_location = app_info.get('STREAMLIT_ROOT_LOCATION')
        if not root_location:
            print("\nNo root location found for the app")
            return
        
        # List files in the stage
        print("\nApp Files:")
        print("=" * 80)
        cursor.execute(f"LIST @{root_location}")
        
        # Get the column names for the LIST command
        file_columns = [desc[0] for desc in cursor.description]
        
        # Print file details
        for row in cursor.fetchall():
            file_info = dict(zip(file_columns, row))
            print(f"\nFile: {file_info.get('name', 'Unknown')}")
            print("-" * 40)
            for col, val in file_info.items():
                if col != 'name':  # Skip name as we already printed it
                    print(f"{col:20}: {val}")
        
        cursor.close()
        
    except Exception as e:
        print(f"Error getting app files: {str(e)}")
        sys.exit(1)

def main():
    app_name = "J1UHFX61NZNBMH9X"  # The app name from your previous output
    
    print(f"Getting details for Streamlit app: {app_name}")
    conn = connect_to_snowflake()
    get_app_files(conn, app_name)
    conn.close()

if __name__ == "__main__":
    main() 