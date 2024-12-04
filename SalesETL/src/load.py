from sqlalchemy import create_engine
import pandas as pd
import configparser
from azure.storage.blob import BlobServiceClient
import io

# Load configuration
config = configparser.ConfigParser(interpolation=None)
config.read('src\config.cfg',encoding='utf-8')
print(config.sections())

# Database configuration
DATABASE_URL = config['database']['uri']
engine = create_engine(DATABASE_URL)

# Azure Blob Storage configuration
ACCOUNT_URL = config['azure']['STORAGE_ACCOUNT_URL']
CONTAINER_NAME = config['azure']['CONTAINER_NAME']
SAS_TOKEN = config['azure']['SAS_TOKEN']

# Functions
def load_sales_data(clean_data: pd.DataFrame):
    """
    Load cleaned sales data to the database.
    """
    try:
        clean_data.to_sql('sales_data', engine, if_exists='append', index=False)
        print("Sales data loaded successfully to the database.")
    except Exception as e:
        print(f"Error loading sales data to database: {e}")


def load_agg_data(agg_data: pd.DataFrame):
    """
    Load aggregated sales data (yearly and monthly) to the database.
    """
    try:
        agg_data.to_sql('yearly_monthly_sales', engine, if_exists='append', index=False)
        print("Aggregated data loaded successfully to the database.")
    except Exception as e:
        print(f"Error loading aggregated data to database: {e}")


def load_to_azure(df: pd.DataFrame,blob_name:str):
    """
    Load a pandas DataFrame to Azure Blob Storage as a CSV file.
    """
    try:
        # Convert DataFrame to CSV in-memory
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

       
        blob_service_client = BlobServiceClient(account_url=ACCOUNT_URL, credential=SAS_TOKEN)
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)

      
        blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)
        print(f"DataFrame uploaded successfully to Azure Blob Storage at {CONTAINER_NAME}/{blob_name}")

    except Exception as e:
        print(f"Error uploading data to Azure: {e}")



