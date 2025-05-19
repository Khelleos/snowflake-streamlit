import os
import sys
import snowflake.connector
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
load_dotenv()

class SnowflakeDeployer:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.app_name = "snowflake_data_explorer"
        self.stage_name = f"{self.app_name}_stage"
        self.warehouse_name = "compute_wh"
        self.database_name = "streamlit_db"
        self.schema_name = "public"
        
        # Set up paths
        self.root_dir = Path(__file__).parent.parent
        self.app_dir = self.root_dir / "streamlit_app"
        self.scripts_dir = self.root_dir / "scripts"

    def execute_sql(self, sql, error_msg):
        """Execute SQL command with error handling"""
        try:
            self.cursor.execute(sql)
            return True
        except Exception as e:
            print(f"{error_msg}: {str(e)}")
            return False

    def setup_infrastructure(self):
        """Set up all required Snowflake infrastructure"""
        # Create warehouse
        self.execute_sql(
            f"CREATE WAREHOUSE IF NOT EXISTS {self.warehouse_name} WAREHOUSE_SIZE = 'X-SMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE",
            "Error creating warehouse"
        )
        
        # Create database and schema
        self.execute_sql(f"CREATE DATABASE IF NOT EXISTS {self.database_name}", "Error creating database")
        self.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {self.database_name}.{self.schema_name}", "Error creating schema")
        
        # Create and configure stage
        stage_name = f"{self.database_name}.{self.schema_name}.{self.stage_name}"
        self.execute_sql(f"CREATE STAGE IF NOT EXISTS {stage_name}", "Error creating stage")
        self.execute_sql(
            f"GRANT READ, WRITE ON STAGE {stage_name} TO ROLE {os.getenv('SNOWFLAKE_ROLE')}",
            "Error granting privileges"
        )
        
        return stage_name

    def upload_files(self, stage_name):
        """Upload and verify application files"""
        # Clean stage
        self.execute_sql(f"REMOVE @{stage_name}", "Error cleaning stage")
        
        # Upload files
        files_to_upload = ['streamlit_app.py', 'environment.yml']
        for file in files_to_upload:
            src_file = self.app_dir / file
            if src_file.exists():
                print(f"Uploading {src_file}")
                self.execute_sql(
                    f"PUT file://{src_file} @{stage_name} AUTO_COMPRESS = FALSE OVERWRITE = TRUE",
                    f"Error uploading {file}"
                )
            else:
                print(f"Warning: {file} not found")

        # Verify uploads
        self.cursor.execute(f"LIST @{stage_name}")
        files = self.cursor.fetchall()
        if not files:
            raise Exception("No files found in stage")
        
        print("\nUploaded files:")
        for file in files:
            print(f"  {file[0]}")

    def create_app(self, stage_name):
        """Create Streamlit application"""
        app_path = f"{self.database_name}.{self.schema_name}.{self.app_name}"
        self.execute_sql(
            f"""
            CREATE OR REPLACE STREAMLIT {app_path}
            ROOT_LOCATION = '@{stage_name}'
            MAIN_FILE = 'streamlit_app.py'
            QUERY_WAREHOUSE = {self.warehouse_name}
            """,
            "Error creating application"
        )
        
        # Get app URL
        account = os.getenv('SNOWFLAKE_ACCOUNT')
        url = f"https://{account}.snowflakecomputing.com/streamlit/{self.database_name}/{self.schema_name}/{self.app_name}"
        print(f"\nYour Streamlit app is available at: {url}")

    def deploy(self):
        """Execute the complete deployment process"""
        try:
            print("Starting deployment...")
            
            # Connect to Snowflake
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
            print("Connected to Snowflake")
            
            # Deploy
            stage_name = self.setup_infrastructure()
            self.upload_files(stage_name)
            self.create_app(stage_name)
            
            print("Deployment completed successfully!")
            
        except Exception as e:
            print(f"Deployment failed: {str(e)}")
        finally:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            print("Cleanup completed")

if __name__ == "__main__":
    SnowflakeDeployer().deploy() 