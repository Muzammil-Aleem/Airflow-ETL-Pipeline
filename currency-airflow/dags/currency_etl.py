import logging
import math
import os

from datetime import date, datetime, timedelta, timezone

import pendulum
import requests

from airflow.sdk import DAG, get_current_context
from airflow.providers.standard.operators.python import PythonOperator
from pymongo import ASCENDING, MongoClient, UpdateOne


LOG = logging.getLogger(__name__)

TARGETS = {"USD", "GBP", "JPY"}


def extract_currency_rates():
    response = requests.get(
        "https://api.frankfurter.dev/v2/rates",
        params={
            "base": "EUR",
            "quotes": "USD,GBP,JPY",
        },
        timeout=30,
    )

    response.raise_for_status()
    rows = response.json()

    if not isinstance(rows, list) or not rows:
        raise ValueError("Expected a nonempty list from Frankfurter")

    payload = {
        "rows": rows,
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
    }

    ti = get_current_context()["ti"]

    ti.xcom_push(
        key="raw_exchange_rates",
        value=payload,
    )

    LOG.info("Extracted data: %s", payload)


def transform_exchange_rates():
    ti = get_current_context()["ti"]

    payload = ti.xcom_pull(
        task_ids="extract_currency_rates",
        key="raw_exchange_rates",
    )

    if not payload or not payload.get("rows"):
        raise ValueError("No extracted data found")

    documents = []
    seen = set()

    for row in payload["rows"]:
        base = str(row["base"]).upper()
        target = str(row["quote"]).upper()

        if isinstance(row["rate"], bool):
            raise ValueError("Boolean rate is invalid")

        rate = float(row["rate"])
        rate_date = date.fromisoformat(row["date"]).isoformat()

        if base != "EUR" or target not in TARGETS:
            raise ValueError(f"Unexpected currency pair: {base}/{target}")

        if not math.isfinite(rate) or rate <= 0:
            raise ValueError("Exchange rate must be positive and finite")

        if target in seen:
            raise ValueError(f"Duplicate currency: {target}")

        seen.add(target)

        documents.append({
            "base_currency": base,
            "target_currency": target,
            "exchange_rate": rate,
            "rate_date": rate_date,
            "source": "Frankfurter API",
            "retrieved_at": payload["retrieved_at"],
        })

    if seen != TARGETS:
        raise ValueError(f"Missing currencies: {TARGETS - seen}")

    ti.xcom_push(
        key="transformed_exchange_rates",
        value=documents,
    )

    LOG.info("Transformed data: %s", documents)


def load_to_mongodb():
    ti = get_current_context()["ti"]

    documents = ti.xcom_pull(
        task_ids="transform_exchange_rates",
        key="transformed_exchange_rates",
    )

    if not documents:
        raise ValueError("No transformed documents found")

    with MongoClient(
        os.environ["MONGODB_URI"],
        serverSelectionTimeoutMS=10000,
    ) as client:
        client.admin.command("ping")

        collection = client["currency_etl"]["exchange_rates"]

        collection.create_index(
            [
                ("base_currency", ASCENDING),
                ("target_currency", ASCENDING),
                ("rate_date", ASCENDING),
            ],
            unique=True,
            name="unique_daily_currency_pair",
        )

        operations = []

        for document in documents:
            query = {
                "base_currency": document["base_currency"],
                "target_currency": document["target_currency"],
                "rate_date": document["rate_date"],
            }

            operations.append(
                UpdateOne(
                    query,
                    {"$set": document},
                    upsert=True,
                )
            )

        result = collection.bulk_write(operations)

        summary = {
            "processed": len(documents),
            "matched": result.matched_count,
            "modified": result.modified_count,
            "upserted": result.upserted_count,
        }

        ti.xcom_push(
            key="mongo_load_summary",
            value=summary,
        )

        LOG.info("MongoDB load complete: %s", summary)


with DAG(
    dag_id="daily_currency_etl",
    description="Daily Frankfurter exchange rates to MongoDB",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 18 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "student",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["assignment3", "etl", "mongodb"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_currency_rates",
        python_callable=extract_currency_rates,
        do_xcom_push=False,
    )

    transform_task = PythonOperator(
        task_id="transform_exchange_rates",
        python_callable=transform_exchange_rates,
        do_xcom_push=False,
    )

    load_task = PythonOperator(
        task_id="load_to_mongodb",
        python_callable=load_to_mongodb,
        do_xcom_push=False,
    )

    extract_task >> transform_task >> load_task
