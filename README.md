# Snowflake Streamlit Project

This project contains a Streamlit application for exploring Snowflake data and deployment scripts for automating the deployment process.

## Project Structure

```
.
├── streamlit_app/           # Streamlit application
│   ├── app.py              # Main Streamlit application
│   ├── requirements.txt    # Application dependencies
│   └── README.md          # Application documentation
│
├── scripts/                # Deployment and utility scripts
│   ├── deploy.py          # Snowflake deployment script
│   └── README.md          # Scripts documentation
│
└── .env                   # Environment variables (not tracked in git)
```

## Setup

1. Create a `.env` file in the root directory with your Snowflake credentials:
```
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_ROLE=your_role
```

2. Install dependencies for both the application and scripts:
```bash
# Install application dependencies
cd streamlit_app
pip install -r requirements.txt

# Install script dependencies
cd ../scripts
pip install -r requirements.txt
```

## Running the Application Locally

1. Navigate to the streamlit_app directory:
```bash
cd streamlit_app
```

2. Run the Streamlit app:
```bash
streamlit run app.py
```

## Deploying to Snowflake

1. Navigate to the scripts directory:
```bash
cd scripts
```

2. Run the deployment script:
```bash
python deploy.py
```

The deployment script will:
- Create necessary Snowflake objects (warehouse, database, schema)
- Package the Streamlit application
- Deploy it to Snowflake
- Set up required permissions

## Documentation

- For application details, see `streamlit_app/README.md`
- For deployment details, see `scripts/README.md`

## Security

- Never commit the `.env` file to version control
- Keep your Snowflake credentials secure
- Use appropriate role-based access control in Snowflake

## Support

For any issues or questions, please refer to the documentation in the respective directories or contact your Snowflake administrator. 