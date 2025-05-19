import os
import sys
import snowflake.connector
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

class StreamlitVersionManager:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.app_name = "snowflake_data_explorer"
        self.database_name = "streamlit_db"
        self.schema_name = "public"
        
    def connect(self):
        """Establish connection to Snowflake"""
        try:
            self.conn = snowflake.connector.connect(
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                user=os.getenv('SNOWFLAKE_USER'),
                password=os.getenv('SNOWFLAKE_PASSWORD'),
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
                database=os.getenv('SNOWFLAKE_DATABASE'),
                schema=os.getenv('SNOWFLAKE_SCHEMA'),
                role=os.getenv('SNOWFLAKE_ROLE')
            )
            self.cursor = self.conn.cursor()
            print("Successfully connected to Snowflake!")
        except Exception as e:
            print(f"Error connecting to Snowflake: {str(e)}")
            sys.exit(1)

    def get_app_history(self):
        """Get the history of the Streamlit app"""
        try:
            app_path = f"{self.database_name}.{self.schema_name}.{self.app_name}"
            print(self.app_name.upper())
            
            # Get app details
            self.cursor.execute(f"""
                SELECT 
                    STREAMLIT_NAME,
                    STREAMLIT_CATALOG,
                    STREAMLIT_schema,
                    STREAMLIT_OWNER,
                    CREATED,
                    LAST_ALTERED,
                    COMMENT
                FROM {self.database_name}.INFORMATION_SCHEMA.STREAMLITS
                WHERE STREAMLIT_NAME = '{self.app_name.upper()}'
            """)
            
            app_info = self.cursor.fetchone()
            if not app_info:
                print("App not found")
                return
            
            print("\nStreamlit App Details:")
            print("=" * 80)
            print(f"Name: {app_info[0]}")
            print(f"Database: {app_info[1]}")
            print(f"Schema: {app_info[2]}")
            print(f"Owner: {app_info[3]}")
            print(f"Created: {app_info[4]}")
            print(f"Last Modified: {app_info[5]}")
            if app_info[6]:
                print(f"Description: {app_info[6]}")
            
            # Get stage files history
            stage_name = f"{self.database_name}.{self.schema_name}.{self.app_name}_stage"
            print("\nStage Files:")
            print("=" * 80)
            self.cursor.execute(f"LIST @{stage_name}")
            files = self.cursor.fetchall()
            
            if files:
                for file in files:
                    print(f"File: {file[0]}")
                    print(f"Size: {file[1]}")
                    print(f"Last Modified: {file[2]}")
                    print("-" * 40)
            else:
                print("No files found in stage")
                
        except Exception as e:
            print(f"Error getting app history: {str(e)}")

    def add_app_comment(self, comment):
        """Add or update the app's description"""
        try:
            app_path = f"{self.database_name}.{self.schema_name}.{self.app_name}"
            self.cursor.execute(f"""
                ALTER STREAMLIT {app_path}
                SET COMMENT = '{comment}'
            """)
            print(f"\nUpdated app description")
            
        except Exception as e:
            print(f"Error updating description: {str(e)}")

    def cleanup(self):
        """Clean up and close connections"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("\nCleanup completed")

def main():
    manager = StreamlitVersionManager()
    try:
        manager.connect()
        
        # Get app history and details
        manager.get_app_history()
        
        # Add a version comment (optional)
        # manager.add_app_comment("Version 1.0.0 - Initial release")
        
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        manager.cleanup()

if __name__ == "__main__":
    main() 