import os
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv()  # Carga variables desde .env

def get_bigquery_client():
    key_path = os.getenv("BIGQUERY_CREDENTIALS_PATH")

    if not key_path or not os.path.exists(key_path):
        raise FileNotFoundError("❌ Archivo de credenciales de BigQuery no encontrado.")

    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    return client