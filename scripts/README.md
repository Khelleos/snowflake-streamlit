# Deployment Scripts

This directory contains scripts for deploying and managing the Snowflake Streamlit application.

## Available Scripts

### deploy.py

The main deployment script that automates the process of deploying the Streamlit application to Snowflake.

#### Features
- Creates necessary Snowflake infrastructure
- Packages the application
- Deploys to Snowflake
- Handles cleanup

#### Usage
```bash
python deploy.py
```

#### Prerequisites
1. Snowflake account with admin privileges
2. Python 3.8 or higher
3. Required Python packages installed (from requirements.txt)
4. Properly configured `.env` file in the root directory

#### What the Script Does

1. **Infrastructure Setup:**
   - Creates an X-SMALL warehouse
   - Creates a dedicated database and schema
   - Sets up application package and stage

2. **Application Packaging:**
   - Creates a temporary package directory
   - Copies necessary files from streamlit_app
   - Creates a ZIP package
   - Cleans up temporary files

3. **Deployment:**
   - Uploads the package to Snowflake stage
   - Creates the application from the package
   - Sets up necessary permissions

4. **Cleanup:**
   - Removes temporary files
   - Closes database connections

## Installation

1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

## Error Handling

The scripts include comprehensive error handling for:
- Connection failures
- Infrastructure creation issues
- Package creation and upload problems
- Application deployment errors

All errors are logged with descriptive messages to help with troubleshooting.

## Security

- Credentials are managed through environment variables
- Temporary files are automatically cleaned up
- Database connections are properly closed
- Proper error handling is implemented

## Troubleshooting

If you encounter issues:
1. Check the error message in the console
2. Verify your Snowflake credentials in the `.env` file
3. Ensure you have the necessary permissions in Snowflake
4. Check the Snowflake application logs

## Support

For any issues or questions, please contact your Snowflake administrator or refer to the Snowflake documentation. 