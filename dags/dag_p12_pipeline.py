"""
P12 - Sport Data Solution
DAG Airflow : pipeline ETL complet.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import psycopg2
import os
import json
import requests


def on_failure_slack(context):
    """Alerte Slack quand une tâche échoue."""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    task_id = context.get("task_instance").task_id
    dag_id = context.get("task_instance").dag_id
    error = str(context.get("exception", "Erreur inconnue"))

    message = f"🚨 *PIPELINE EN ÉCHEC*\nDAG: {dag_id}\nTâche: {task_id}\nErreur: {error}"

    if not webhook_url:
        print(f"[ALERTE MOCK] {message}")
        return

    requests.post(
        webhook_url,
        data=json.dumps({"text": message}),
        headers={"Content-Type": "application/json"},
        timeout=10,
    )


default_args = {
    "owner": "p12",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": on_failure_slack,
}


def verify_data(**kwargs):
    """Vérification finale."""
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )
    cur = conn.cursor()

    checks = {
        "raw.salaries": 100,
        "raw.sports_declares": 100,
        "raw.activites_sportives": 1000,
    }

    for table, min_rows in checks.items():
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f"  {table}: {count} lignes")
        if count < min_rows:
            raise ValueError(f"ECHEC: {table} a {count} lignes (minimum: {min_rows})")

    views = [
        "analytics.prime_sportive",
        "analytics.jours_bien_etre",
        "analytics.kpi_global",
    ]

    for view in views:
        cur.execute(f"SELECT COUNT(*) FROM {view}")
        count = cur.fetchone()[0]
        print(f"  {view}: {count} lignes")
        if count == 0:
            raise ValueError(f"ECHEC: la vue {view} est vide")

    cur.execute("SELECT * FROM analytics.kpi_global")
    cols = [desc[0] for desc in cur.description]
    row = cur.fetchone()
    print("\n  === KPI GLOBAL ===")
    for col, val in zip(cols, row):
        print(f"  {col}: {val}")

    cur.close()
    conn.close()
    print("\n  Toutes les verifications OK")


with DAG(
    dag_id="p12_sport_data_pipeline",
    default_args=default_args,
    description="Pipeline ETL complet",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["p12", "sport"],
) as dag:

    load_excel = BashOperator(
        task_id="load_excel",
        bash_command="python /opt/airflow/scripts/load_data.py",
    )

    generate_strava = BashOperator(
        task_id="generate_strava",
        bash_command="python /opt/airflow/scripts/generate_strava.py",
    )

    validate_distances = BashOperator(
        task_id="validate_distances",
        bash_command="python /opt/airflow/scripts/google_maps.py",
    )

    soda_checks = BashOperator(
        task_id="soda_checks",
        bash_command="python /opt/airflow/scripts/run_soda_checks.py",
    )

    slack_notify = BashOperator(
        task_id="slack_notify",
        bash_command="python /opt/airflow/scripts/slack_notify.py",
    )

    verify = PythonOperator(
        task_id="verify_data",
        python_callable=verify_data,
    )

    load_excel >> generate_strava >> validate_distances >> soda_checks >> slack_notify >> verify