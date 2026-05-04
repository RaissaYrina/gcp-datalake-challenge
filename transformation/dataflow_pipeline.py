import os
import requests
import json
from datetime import datetime
from google.cloud import storage, bigquery
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")

def read_weather() -> list:
    "Le todos os arquivos historicos da bronze para cada cidade"
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    blobs = list(bucket.list_blobs(prefix='bronze/weather/'))

    records = []
    for blob in blobs:
        parts = blob.name.split('/')
        if len(parts) == 4:
            data = json.loads(blob.download_as_string())
            # Adiciona o timestamp da extração baseado no nome do arquivo para trazer histórico
            timestamp_str = parts[3].replace(".json", "")
            data["_extraction_timestamp"] = timestamp_str
            records.append(data)

        print(f"{len(records)} arquivos de clima lidos do Bronze")
    return records

def read_latest_cities() -> list:
    "Le o arquivo mais recente das cidades da camada bronze."
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    blobs = sorted(
        bucket.list_blobs(prefix='bronze/cities/'),
        key = lambda b:b.updated,
        reverse=True
    )

    if not blobs:
        raise Exception('Nenhum arquivo de cidades encontrado no Bronze!')
    
    data = json.loads(blobs[0].download_as_string())
    print(f"{len(data)} cidades lidas do Bronze")
    return data

def transform_weather(weather_records: list, cities_records: list) -> list:
    "Limpa, valida e cruza os dados de clima com metadados das cidades."

    #cria dicionarios de cidades pelo nome para cruzamento
    cities_dict = {c['cidade'].lower(): c for c in cities_records}

    transformed = []
    for record in weather_records:
        try:
            city_name = record['name'].lower()
            city_meta = cities_dict.get(city_name, {})

            # Dados de temperatura
            temp = record["main"]["temp"]
            temp_min_alerta = city_meta.get("temp_min_alerta", 0)
            temp_max_alerta = city_meta.get("temp_max_alerta", 40)
            umidade = record["main"]["humidity"]
            umidade_max_alerta = city_meta.get("umidade_max_alerta", 90)

            transformed.append({
                # Identificação
                "cidade": record["name"],
                "estado": city_meta.get("estado", "N/A"),
                "regiao": city_meta.get("regiao", "N/A"),
                "populacao": city_meta.get("populacao", 0),
                "latitude": record["coord"]["lat"],
                "longitude": record["coord"]["lon"],

                # Clima
                "temperatura": temp,
                "sensacao_termica": record["main"]["feels_like"],
                "temp_minima": record["main"]["temp_min"],
                "temp_maxima": record["main"]["temp_max"],
                "umidade": umidade,
                "pressao": record["main"]["pressure"],
                "visibilidade": record.get("visibility", 0),
                "descricao_clima": record["weather"][0]["description"],
                "velocidade_vento": record["wind"]["speed"],
                "nuvens": record["clouds"]["all"],

                # Alertas
                "alerta_temp_baixa": temp < temp_min_alerta,
                "alerta_temp_alta": temp > temp_max_alerta,
                "alerta_umidade": umidade > umidade_max_alerta,

                # Controle
                "timestamp_extracao": datetime.utcnow().isoformat(),
                "data_particao": datetime.utcnow().strftime("%Y-%m-%d")
            })
        except Exception as e:
            print(f"Erro ao transformar {record.get('name', 'desconhecida')}: {e}")
    
    print(f"{len(transformed)} registros transformados")
    return transformed

def save_to_silver(data: list):
    """Salva os dados transformados no BigQuery — camada Silver."""
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.silver.weather_cities"

    # Schema da tabela
    schema = [
        bigquery.SchemaField("cidade", "STRING"),
        bigquery.SchemaField("estado", "STRING"),
        bigquery.SchemaField("regiao", "STRING"),
        bigquery.SchemaField("populacao", "INTEGER"),
        bigquery.SchemaField("latitude", "FLOAT"),
        bigquery.SchemaField("longitude", "FLOAT"),
        bigquery.SchemaField("temperatura", "FLOAT"),
        bigquery.SchemaField("sensacao_termica", "FLOAT"),
        bigquery.SchemaField("temp_minima", "FLOAT"),
        bigquery.SchemaField("temp_maxima", "FLOAT"),
        bigquery.SchemaField("umidade", "INTEGER"),
        bigquery.SchemaField("pressao", "INTEGER"),
        bigquery.SchemaField("visibilidade", "INTEGER"),
        bigquery.SchemaField("descricao_clima", "STRING"),
        bigquery.SchemaField("velocidade_vento", "FLOAT"),
        bigquery.SchemaField("nuvens", "INTEGER"),
        bigquery.SchemaField("alerta_temp_baixa", "BOOLEAN"),
        bigquery.SchemaField("alerta_temp_alta", "BOOLEAN"),
        bigquery.SchemaField("alerta_umidade", "BOOLEAN"),
        bigquery.SchemaField("timestamp_extracao", "TIMESTAMP"),
        bigquery.SchemaField("data_particao", "DATE"),
    ]

    # Cria tabela se não existir
    try:
        client.get_table(table_id)
        print(f"Tabela {table_id} já existe")
    except Exception:
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)
        print(f"Tabela {table_id} criada!")

    # Insere os dados
    errors = client.insert_rows_json(table_id, data)
    if errors:
        raise Exception(f"Erros ao inserir no BigQuery: {errors}")
    
    print(f"{len(data)} registros salvos no BigQuery Silver!")

def run():
    """Executa o pipeline de transformação Bronze → Silver."""
    print("Iniciando pipeline de transformação...")

    try:
        # Lê do Bronze
        weather_data = read_weather()
        cities_data = read_latest_cities()

        # Transforma
        transformed = transform_weather(weather_data, cities_data)

        # Salva no Silver
        save_to_silver(transformed)

        print("Pipeline concluído com sucesso!")
    except Exception as e:
        print(f"Erro no pipeline: {e}")

if __name__ == "__main__":
    run()



