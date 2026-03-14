"""
P12 - Sport Data Solution
DAG Airflow : orchestre le pipeline ETL complet.
Airflow ne calcule rien — il lance les scripts dans le bon ordre.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import psycopg2
import os


# --- Configuration du DAG ---
default_args = {
    "owner": "p12",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def verify_data(**kwargs):
    """
    Tâche de vérification : compte les lignes dans chaque table raw
    et vérifie que les vues analytics retournent des résultats.
    Si un check échoue, la tâche lève une exception → le DAG s'arrête.
    """
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )
    cur = conn.cursor()

    checks = {
        "raw.salaries": 100,              # minimum 100 salariés attendus
        "raw.sports_declares": 100,        # minimum 100 déclarations
        "raw.activites_sportives": 1000,   # minimum 1000 activités
    }

    for table, min_rows in checks.items():
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f"  {table}: {count} lignes")
        if count < min_rows:
            raise ValueError(f"ECHEC: {table} a {count} lignes (minimum attendu: {min_rows})")

    # Vérifier que les vues analytics fonctionnent
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

    # Afficher les KPI
    cur.execute("SELECT * FROM analytics.kpi_global")
    cols = [desc[0] for desc in cur.description]
    row = cur.fetchone()
    print("\n  === KPI GLOBAL ===")
    for col, val in zip(cols, row):
        print(f"  {col}: {val}")

    cur.close()
    conn.close()
    print("\n  Toutes les vérifications OK")


# --- Définition du DAG ---
with DAG(
    dag_id="p12_sport_data_pipeline",
    default_args=default_args,
    description="Pipeline ETL complet : ingestion Excel + génération Strava + validation distances",
    schedule_interval=None,  # Déclenchement manuel uniquement (POC)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["p12", "sport"],
) as dag:

    # Tâche 1 : Charger les fichiers Excel dans raw
    load_excel = BashOperator(
        task_id="load_excel",
        bash_command="python /opt/airflow/scripts/load_data.py",
    )

    # Tâche 2 : Générer les activités sportives simulées
    generate_strava = BashOperator(
        task_id="generate_strava",
        bash_command="python /opt/airflow/scripts/generate_strava.py",
    )

    # Tâche 3 : Calculer les distances domicile → Lattes (Google Maps)
    validate_distances = BashOperator(
        task_id="validate_distances",
        bash_command="python /opt/airflow/scripts/google_maps.py",
    )

    # Tâche 4 : Vérifier l'intégrité des données
    verify = PythonOperator(
        task_id="verify_data",
        python_callable=verify_data,
    )

    # --- Ordre d'exécution ---
    # load_excel PUIS generate_strava (les activités ont besoin des salariés en base)
    # PUIS validate_distances (besoin des adresses en base)
    # PUIS verify (vérifie que tout est OK)
    load_excel >> generate_strava >> validate_distances >> verify
