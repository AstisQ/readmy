"""
#  DAG: Car Brands API → S3

## 📌 Описание
DAG загружает данные по автомобильным брендам из API DaData,
обогащает их локальными справочниками и сохраняет в S3 в формате Parquet.

---

##  Логика пайплайна

1. **Extract**
   - Запрос к API DaData по списку брендов
   - Получение информации о бренде

2. **Transform**
   - Добавление:
     - страны (`country_map`)
     - моделей (`models_map`)
     - года (`year_map`)
 - Разворачивание моделей (`explode`)
   - Добавление `business_date`

3. **Load**
   - Сохранение в Parquet
   - Загрузка в S3 (bucket: `prod`)

---

##  Бизнес-дата
- Используется `ds` из Airflow
- Формат: `YYYY-MM-DD`
- Добавляется в данные как `business_date`

---

##  Выходные данные

Файл в S3:

"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

import requests
import pandas as pd
import os
from datetime import datetime
import logging
import hashlib
import io


URL = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/suggest/car_brand"

brands = [
    "bmw",
    "audi",
    "toyota",
    "mercedes",
    "honda",
    "nissan",
    "lada",
    "ford",
    "chevrolet",
    "volvo",
]

country_map = {
    "BMW": "Germany",
    "AUDI": "Germany",
    "TOYOTA": "Japan",
    "HONDA": "Japan",
    "NISSAN": "Japan",
    "MERCEDES": "Germany",
    "VAZ": "Russia",
    "FORD": "Usa",
    "CHEVROLET": "Usa",
    "VOLVO": "Sweden",
}

models_map = {
    "BMW": ["X5", "X3", "3 Series"],
    "AUDI": ["A4", "A6", "Q5"],
    "TOYOTA": ["Camry", "Corolla", "RAV4"],
    "HONDA": ["Civic", "Accord", "CR-V"],
    "NISSAN": ["Altima", "Qashqai", "X-Trail"],
    "MERCEDES": ["C-Class", "E-Class", "GLE"],
    "VAZ": ["Vesta", "Granta", "Niva"],
    "FORD": ["Focus", "Mustang", "Explorer"],
    "CHEVROLET": ["Malibu", "Camaro", "Tahoe"],
    "VOLVO": ["XC60", "XC90", "S60"],
}

year_map = {
    "BMW": 2020,
    "AUDI": 2021,
    "TOYOTA": 2023,
    "HONDA": 2019,
    "NISSAN": 2021,
    "MERCEDES": 2023,
    "VAZ": 2022,
    "FORD": 2020,
    "CHEVROLET": 2021,
    "VOLVO": 2023,
}


def api_download(**context):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    business_date_str = context.get("ds")
    if business_date_str:
        business_date = datetime.strptime(business_date_str, "%Y-%m-%d").date()
    else:
        business_date = datetime.utcnow().date()

    api_key = Variable.get("DADATA_API_KEY", default_var=None)

    if not api_key:
        raise ValueError("DADATA_API_KEY is not set")

    logger.info(f"API key loaded: {api_key[:5]}***")

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Token {api_key}",
    }

    rows = []

    for b in brands:
        response = requests.post(
            URL,
            headers=headers,
            json={"query": b},
            timeout=5,
        )

        if response.status_code != 200:
            logger.error(f"Error for brand {b}: {response.status_code}")
            continue

        result = response.json()
        suggestions = result.get("suggestions", [])
        if not suggestions:
            logger.warning(f"No suggestions for brand {b}")
            continue

        item = suggestions[0]
        d = item.get("data", {})

        rows.append(
            {
                "query": b,
                "value": item.get("value"),
                "id": d.get("id"),
                "name": d.get("name"),
                "name_ru": d.get("name_ru"),
            }
        )

    df = pd.DataFrame(rows)

    if df.empty:
        logger.warning("No data received from API")
    else:
        df["country"] = df["id"].map(country_map)
        df["models"] = df["id"].map(models_map)
        df["year"] = df["id"].map(year_map)
        df["business_date"] = business_date

        df = df.explode("models")
        df = df.sort_values("country")

    filename = f"pasha_7701/{business_date}.parquet"

    if not df.empty:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        hook = S3Hook(aws_conn_id="minios3_conn")
        hook.load_bytes(
            bytes_data=buffer.read(),
            key=filename,
            bucket_name="dev",
            replace=True,
        )
        logger.info(f"Uploaded to S3: {filename}")

    logger.info(f"Dataset created with {len(df)} rows")


default_args = {
    "owner": "pasha_7701",
    "start_date": days_ago(7),
    "retries": 2,
}

dag = DAG(
    dag_id="pasha_7701_Astakhov_API_to_S3__cars",
    default_args=default_args,
    schedule_interval="0 10 * * *",
    catchup=True,
    description="API to S3 (MinIO)",
    tags=["api", "s3", "cars"],
)

upload_api_to_s3 = PythonOperator(
    task_id="upload_api_to_s3",
    python_callable=api_download,
    dag=dag,
)

upload_api_to_s3
dag.doc_md = __doc__
