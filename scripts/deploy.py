import os
import sys
import snowflake.connector
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
load_dotenv()

class SnowflakeDeployer:
    def __init__(self, app_path):
        self.conn = None
        self.cursor = None
        self.app_dir = Path(app_path)
        if not self.app_dir.exists():
            raise ValueError(f"Streamlit app directory not found: {app_path}")
            
        # Use folder name as app name
        self.app_name = self.app_dir.name
        self.database_name = "streamlit_db"
        self.schema_name = "public"
        self.stage_name = f"{self.database_name}.{self.schema_name}.{self.app_name}_stage"

    def execute_sql(self, sql):
        """Execute SQL command with error handling"""
        try:
            self.cursor.execute(sql)
            return True
        except Exception as e:
            print(f"Error executing SQL: {str(e)}")
            return False

    def setup_infrastructure(self):
        """Set up Snowflake infrastructure"""
        print("\nSetting up Snowflake infrastructure...")
        
        # Create warehouse if not exists
        self.execute_sql(f"""
            CREATE WAREHOUSE IF NOT EXISTS {os.getenv('SNOWFLAKE_WAREHOUSE')}
            WITH WAREHOUSE_SIZE = 'XSMALL'
            AUTO_SUSPEND = 60
            AUTO_RESUME = TRUE
        """)
        
        # Create database if not exists
        self.execute_sql(f"CREATE DATABASE IF NOT EXISTS {self.database_name}")
        
        # Create schema if not exists
        self.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {self.database_name}.{self.schema_name}")
        
        # Create stage if not exists
        self.execute_sql(f"""
            CREATE STAGE IF NOT EXISTS {self.stage_name}
            DIRECTORY = (ENABLE = TRUE)
        """)
        
        # Grant privileges
        self.execute_sql(f"GRANT USAGE ON WAREHOUSE {os.getenv('SNOWFLAKE_WAREHOUSE')} TO ROLE {os.getenv('SNOWFLAKE_ROLE')}")
        self.execute_sql(f"GRANT USAGE ON DATABASE {self.database_name} TO ROLE {os.getenv('SNOWFLAKE_ROLE')}")
        self.execute_sql(f"GRANT USAGE ON SCHEMA {self.database_name}.{self.schema_name} TO ROLE {os.getenv('SNOWFLAKE_ROLE')}")
        self.execute_sql(f"GRANT ALL ON STAGE {self.stage_name} TO ROLE {os.getenv('SNOWFLAKE_ROLE')}")

    def upload_files(self):
        """Upload files to Snowflake stage"""
        print("\nUploading files to Snowflake stage...")
        
        # Clean stage
        self.execute_sql(f"REMOVE @{self.stage_name}")
        
        # Get all Python files and environment.yml in the app directory and its subdirectories
        files_to_upload = []
        for file_path in self.app_dir.rglob("*"):
            if file_path.is_file() and (file_path.suffix == '.py' or file_path.name == 'environment.yml'):
                files_to_upload.append(file_path)
        
        if not files_to_upload:
            print("No Python files or environment.yml found in the app directory!")
            return False
            
        # Upload each file
        for file_path in files_to_upload:
            relative_path = file_path.relative_to(self.app_dir)
            print(f"Uploading {relative_path}...")
            self.execute_sql(f"PUT file://{file_path} @{self.stage_name}/{relative_path.parent} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
            
        return True

    def verify_uploads(self):
        """Verify that files were uploaded correctly"""
        print("\nVerifying file uploads...")
        self.cursor.execute(f"LIST @{self.stage_name}")
        files = self.cursor.fetchall()
        
        if not files:
            print("No files found in stage!")
            return False
            
        print("\nUploaded files:")
        for file in files:
            print(f"- {file[0]}")
            
        return True

    def create_streamlit_app(self):
        """Create the Streamlit app"""
        print("\nCreating Streamlit app...")
        
        # Get the main app file path
        main_app_path = self.app_dir / "streamlit_app.py"
        if not main_app_path.exists():
            print("Main app file not found!")
            return False
            
        # Create the Streamlit app
        app_path = f"{self.database_name}.{self.schema_name}.{self.app_name}"
        
        self.execute_sql(f"""
            CREATE OR REPLACE STREAMLIT {app_path}
            ROOT_LOCATION = '@{self.stage_name}'
            MAIN_FILE = 'streamlit_app.py'
            QUERY_WAREHOUSE = {os.getenv('SNOWFLAKE_WAREHOUSE')}
        """)
        
        # Get the app URL
        self.cursor.execute(f"SHOW STREAMLITS LIKE '{self.app_name}'")
        app_info = self.cursor.fetchone()
        if app_info:
            print(f"\nStreamlit app created successfully!")
            print(f"App URL: {app_info[2]}")
            return True
            
        return False

    def deploy(self):
        """Deploy the Streamlit app"""
        try:
            print(f"Starting deployment of {self.app_name}...")
            
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
            self.setup_infrastructure()
            
            if self.upload_files() and self.verify_uploads():
                self.create_streamlit_app()
            
        except Exception as e:
            print(f"Error during deployment: {str(e)}")
        finally:
            if self.conn:
                self.conn.close()
            print("Cleanup completed")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python deploy.py <path_to_streamlit_app>")
        sys.exit(1)
        
    app_path = sys.argv[1]
    SnowflakeDeployer(app_path).deploy() 