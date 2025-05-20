import os
import sys
import snowflake.connector
from dotenv import load_dotenv
from pathlib import Path
import yaml
import time

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
        self.database_name = os.getenv('SNOWFLAKE_DATABASE')
        self.schema_name = os.getenv('SNOWFLAKE_SCHEMA')
        self.stage_name = f"{self.database_name}.{self.schema_name}.{self.app_name}_stage"
        
        # Validate environment variables
        self._validate_env_vars()
        
    def _validate_env_vars(self):
        """Validate required environment variables"""
        required_vars = [
            'SNOWFLAKE_ACCOUNT',
            'SNOWFLAKE_USER',
            'SNOWFLAKE_PASSWORD',
            'SNOWFLAKE_WAREHOUSE',
            'SNOWFLAKE_DATABASE',
            'SNOWFLAKE_SCHEMA',
            'SNOWFLAKE_ROLE'
        ]
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

    def _validate_app_structure(self):
        """Validate the Streamlit app structure"""
        # Check for main app file
        main_app = self.app_dir / "streamlit_app.py"
        if not main_app.exists():
            raise ValueError("Main app file (streamlit_app.py) not found")
            
        # Check for environment.yml
        env_file = self.app_dir / "environment.yml"
        if not env_file.exists():
            raise ValueError("environment.yml not found")
            
        # Validate environment.yml format
        try:
            with open(env_file, 'r') as f:
                yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid environment.yml format: {str(e)}")

    def execute_sql(self, sql, retries=3, delay=1):
        """Execute SQL command with retry logic"""
        for attempt in range(retries):
            try:
                self.cursor.execute(sql)
                return True
            except snowflake.connector.errors.ProgrammingError as e:
                if attempt == retries - 1:
                    print(f"Error executing SQL: {str(e)}")
                    return False
                time.sleep(delay)
            except Exception as e:
                print(f"Error executing SQL: {str(e)}")
                return False

    def setup_stage(self):
        """Set up stage for the app"""
        print("\nSetting up stage...")
        
        # Create stage if not exists
        self.execute_sql(f"""
            CREATE STAGE IF NOT EXISTS {self.stage_name}
            DIRECTORY = (ENABLE = TRUE)
        """)
        
        # Grant privileges
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
            
        # Verify all required files are present
        required_files = ['streamlit_app.py', 'environment.yml']
        uploaded_files = [f[0].split('/')[-1] for f in files]
        missing_files = [f for f in required_files if f not in uploaded_files]
        
        if missing_files:
            print(f"Missing required files: {', '.join(missing_files)}")
            return False
            
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
        
        # Get allowed roles from environment
        allowed_roles = os.getenv('ALLOWED_ROLES', '').split(',')
        roles_str = ','.join(role.strip() for role in allowed_roles if role.strip())
        
        # First verify all roles exist
        if roles_str:
            print("\nVerifying roles...")
            for role in allowed_roles:
                role = role.strip()
                if role:
                    self.cursor.execute(f"SHOW ROLES LIKE '{role}'")
                    if not self.cursor.fetchone():
                        print(f"Error: Role '{role}' does not exist")
                        return False
        
        # Create app with role information in comment
        self.execute_sql(f"""
            CREATE OR REPLACE STREAMLIT {app_path}
            ROOT_LOCATION = '@{self.stage_name}'
            MAIN_FILE = 'streamlit_app.py'
            QUERY_WAREHOUSE = {os.getenv('SNOWFLAKE_WAREHOUSE')}
            COMMENT = 'Allowed roles: {roles_str}'
        """)
        
        # Grant access to specified roles
        if roles_str:
            try:
                for role in allowed_roles:
                    role = role.strip()
                    if role:
                        self.execute_sql(f"GRANT USAGE ON STREAMLIT {app_path} TO ROLE {role}")
                        print(f"Granted access to role: {role}")
            except Exception as e:
                print(f"Error granting roles: {str(e)}")
                # Rollback: Drop the Streamlit app if role assignment fails
                print("Rolling back: Removing Streamlit app...")
                self.execute_sql(f"DROP STREAMLIT IF EXISTS {app_path}")
                return False
        
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
            
            # Validate app structure
            self._validate_app_structure()
            
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
            self.setup_stage()
            
            if self.upload_files() and self.verify_uploads():
                self.create_streamlit_app()
            
        except Exception as e:
            print(f"Error during deployment: {str(e)}")
            return False
        finally:
            if self.conn:
                self.conn.close()
            print("Cleanup completed")
        return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python deploy.py <path_to_streamlit_app>")
        sys.exit(1)
        
    app_path = sys.argv[1]
    deployer = SnowflakeDeployer(app_path)
    success = deployer.deploy()
    sys.exit(0 if success else 1) 