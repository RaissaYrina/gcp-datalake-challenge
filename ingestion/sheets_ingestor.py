import os
import json
import pytz
from datetime import datetime
from google.cloud import storage
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials

load_dotenv()

BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
SHEETS_ID = os.getenv("GOOGLE_SHEETS_ID")
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly"
]

def get_sheets_data() -> list:
    """Lê os dados da planilha Google Sheets."""
    creds = Credentials.from_service_account_file(
        CREDENTIALS_PATH,
        scopes=SCOPES
    )
    client = gspread.authorize(creds)
    try:
        sheet = client.open_by_key(SHEETS_ID).sheet1
    except gspread.exceptions.APIError as e:
        raise Exception(f"Erro ao acessar planilha: {e.response.status_code} - verifique se compartilhou com a Service Account")
    except gspread.exceptions.SpreadsheetNotFound:
        raise Exception("Planilha não encontrada - verifique o ID no .env")
    
    records = sheet.get_all_records()
    print(f"{len(records)} cidades encontradas na planilha")
    return records

def save_to_bronze(data: list) -> str:
    """Salva os dados da planilha no GCS na camada Bronze."""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    tz_brasilia = pytz.timezone("America/Sao_Paulo")
    timestamp = datetime.now(tz_brasilia).strftime("%Y-%m-%d_%H-%M-%S")
    blob_path = f"bronze/cities/{timestamp}.json"

    blob = bucket.blob(blob_path)
    blob.upload_from_string(
        data=json.dumps(data, ensure_ascii=False, indent=2),
        content_type="application/json"
    )

    print(f"Dados salvos em: gs://{BUCKET_NAME}/{blob_path}")
    return blob_path

def run():
    """Executa a ingestão da planilha Google Sheets."""
    print("Iniciando ingestão do Google Sheets...")

    try:
        data = get_sheets_data()
        save_to_bronze(data)
    except Exception as e:
        print(f" Erro na ingestão: {e}")

if __name__ == "__main__":
    run()