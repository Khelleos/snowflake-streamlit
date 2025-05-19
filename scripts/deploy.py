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

    def create_warehouse(self):
        """Create warehouse for the application"""
        try:
            self.cursor.execute(f"""
                CREATE WAREHOUSE IF NOT EXISTS {self.warehouse_name}
                WAREHOUSE_SIZE = 'X-SMALL'
                AUTO_SUSPEND = 60
                AUTO_RESUME = TRUE
            """)
            print(f"Warehouse {self.warehouse_name} created or already exists")
        except Exception as e:
            print(f"Error creating warehouse: {str(e)}")
            sys.exit(1)

    def create_database_and_schema(self):
        """Create database and schema for the application"""
        try:
            # Create database
            self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database_name}")
            print(f"Database {self.database_name} created or already exists")
            
            # Create schema
            self.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {self.database_name}.{self.schema_name}")
            print(f"Schema {self.schema_name} created or already exists")
            
        except Exception as e:
            print(f"Error creating database and schema: {str(e)}")
            sys.exit(1)

    def create_stage(self):
        """Create stage for the application files"""
        try:
            # Set the current database and schema
            self.cursor.execute(f"USE DATABASE {self.database_name}")
            self.cursor.execute(f"USE SCHEMA {self.schema_name}")
            
            # Create stage with fully qualified name
            stage_name = f"{self.database_name}.{self.schema_name}.{self.stage_name}"
            self.cursor.execute(f"CREATE STAGE IF NOT EXISTS {stage_name}")
            print(f"Stage {stage_name} created or already exists")
            
            # Grant privileges
            self.cursor.execute(f"GRANT READ, WRITE ON STAGE {stage_name} TO ROLE {os.getenv('SNOWFLAKE_ROLE')}")
            print("Privileges granted successfully")
            
        except Exception as e:
            print(f"Error creating stage: {str(e)}")
            sys.exit(1)

    def upload_files(self):
        """Upload application files to the stage"""
        try:
            # Set the current database and schema
            self.cursor.execute(f"USE DATABASE {self.database_name}")
            self.cursor.execute(f"USE SCHEMA {self.schema_name}")
            
            # Create fully qualified stage name
            stage_name = f"{self.database_name}.{self.schema_name}.{self.stage_name}"
            
            # First, remove any existing files in the stage
            self.cursor.execute(f"REMOVE @{stage_name}")
            print("Cleaned existing files from stage")
            
            # Upload files directly to the stage root
            files_to_upload = ['streamlit_app.py', 'environment.yml', 'manifest.yml']
            for file in files_to_upload:
                src_file = self.app_dir / file
                if src_file.exists():
                    print(f"Uploading {src_file} to stage")
                    # Use PUT command with explicit file format and compression settings
                    self.cursor.execute(f"""
                        PUT file://{src_file} @{stage_name}
                        AUTO_COMPRESS = FALSE
                        OVERWRITE = TRUE
                    """)
                else:
                    print(f"Warning: Source file {src_file} does not exist")
            
            # Verify files are in the stage
            print("\nVerifying files in stage:")
            self.cursor.execute(f"LIST @{stage_name}")
            files = self.cursor.fetchall()
            
            if not files:
                raise Exception("No files found in stage after upload")
                
            print("\nFiles in stage:")
            for file in files:
                print(f"  {file[0]}")
                
            # Verify file contents
            print("\nVerifying file contents:")
            for file in files:
                file_name = file[0].split('/')[-1]
                print(f"\nChecking {file_name}:")
                try:
                    # Use the correct syntax for GET command
                    self.cursor.execute(f"SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1 FROM @{stage_name}/{file_name}")
                    content = self.cursor.fetchall()
                    if content:
                        print(f"  Successfully retrieved {file_name}")
                    else:
                        raise Exception(f"No content found in {file_name}")
                except Exception as e:
                    print(f"  Error retrieving {file_name}: {str(e)}")
                    raise Exception(f"Failed to verify file {file_name}")
                
            print("\nFiles uploaded and verified successfully")
            
        except Exception as e:
            print(f"Error uploading files: {str(e)}")
            sys.exit(1)

    def create_application(self):
        """Create the Streamlit application"""
        try:
            # Set the current database and schema
            self.cursor.execute(f"USE DATABASE {self.database_name}")
            self.cursor.execute(f"USE SCHEMA {self.schema_name}")
            
            # Create fully qualified stage name
            stage_name = f"{self.database_name}.{self.schema_name}.{self.stage_name}"
            
            # Create Streamlit application using manifest
            self.cursor.execute(f"""
                CREATE OR REPLACE STREAMLIT {self.database_name}.{self.schema_name}.{self.app_name}
                ROOT_LOCATION = '@{stage_name}'
                MAIN_FILE = 'streamlit_app.py'
                QUERY_WAREHOUSE = {self.warehouse_name}
            """)
            print("Application created successfully")
            
            # Get the application URL using SHOW STREAMLITS
            self.cursor.execute(f"""
                SHOW STREAMLITS LIKE '{self.app_name}'
            """)
            streamlit_info = self.cursor.fetchone()
            if streamlit_info:
                # The URL is typically in the format: https://<account>.snowflakecomputing.com/streamlit/<database>/<schema>/<app_name>
                account = os.getenv('SNOWFLAKE_ACCOUNT')
                url = f"https://{account}.snowflakecomputing.com/streamlit/{self.database_name}/{self.schema_name}/{self.app_name}"
                print(f"\nYour Streamlit app is available at: {url}")
            else:
                print("\nApplication created but URL could not be determined")
            
        except Exception as e:
            print(f"Error creating application: {str(e)}")
            sys.exit(1)

    def cleanup(self):
        """Clean up and close connections"""
        try:
            # Close connections
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            print("Cleanup completed")
        except Exception as e:
            print(f"Error during cleanup: {str(e)}")

    def deploy(self):
        """Execute the complete deployment process"""
        try:
            print("Starting deployment process...")
            self.connect()
            self.create_warehouse()
            self.create_database_and_schema()
            self.create_stage()
            self.upload_files()
            self.create_application()
            print("Deployment completed successfully!")
        except Exception as e:
            print(f"Deployment failed: {str(e)}")
        finally:
            self.cleanup()

if __name__ == "__main__":
    deployer = SnowflakeDeployer()
    deployer.deploy() 