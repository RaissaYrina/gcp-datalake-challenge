import os
import json
import requests
from datetime import datetime
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

#Variaveris de ambiente
API_KEY = os.getenv("OPENWEATHER_API_KEY")
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
CITIES = os.getenv("CITIES").split(",")

def fetch_weather(city: str) -> dict:
    """Busca dados de clima da API OpenWeather para uma cidade."""
    url = f"https://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city,
        "appid": API_KEY,
        "units": "metric",
        "lang": "pt_br"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def save_to_bronze(data: dict, city: str) -> str:
    """Salva os dados brutos no GCS na camada Bronze."""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    #Organiza por cidade e data
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    city_clean = city.strip().lower().replace(" ","_")
    blob_path = f"bronze/weather/{city_clean}/{timestamp}.json"

    blob = bucket.blob(blob_path)
    blob.upload_from_string(
        data=json.dumps(data, ensure_ascii=False, indent=2),
        content_type="application/json"
    )

    print(f"{city} salvo em: gs://{BUCKET_NAME}/{blob_path}")
    return blob_path

def run():
    """Executa a ingestão para todas as cidades."""
    print(f'Iniciando a ingestão de {len(CITIES)} cidades...')

    sucesso = 0
    erro = 0

    for city in CITIES:
        try:
            data = fetch_weather(city.strip())
            save_to_bronze(data,city)
            sucesso += 1
        except Exception as e:
            print(f'Erro em {city}: {e}')
            erro += 1 

    
    print(f"✅ Sucesso: {sucesso} cidades")
    print(f"❌ Erros: {erro} cidades")

if __name__ == "__main__":
    run()


