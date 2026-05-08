# ============================================================
# dags/news_pipeline_dag.py
# DAG Airflow - Orchestration du pipeline News
# EMSI Casablanca - IADATA 2025/2026
# ============================================================

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ── Configuration par défaut ──────────────────────────────────
default_args = {
    'owner': 'iadata',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# ── DAG : toutes les heures (Batch Ingestion) ─────────────────
with DAG(
    dag_id='news_pipeline_hourly',
    default_args=default_args,
    description='Pipeline complet : Scraping → Bronze → Silver → Gold',
    schedule_interval='@hourly',           # Batch toutes les heures
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['news', 'scraping', 'etl', 'iadata'],
) as dag:

    # ── Task 1 : Scraping (Bronze Layer) ─────────────────────
    scraping = BashOperator(
        task_id='scraping_bronze',
        bash_command='cd /app && python scrapers/main_scraper.py',
        doc_md="""
        **Scraping** : Collecte des articles depuis toutes les sources.
        Sources : Hespress, BBC, Al Jazeera, Reuters, Barlamane, Akhbarona
        Sortie : data_lake/bronze/*.json
        """,
    )

    # ── Task 2 : Nettoyage (Silver Layer) ────────────────────
    silver_processing = BashOperator(
        task_id='silver_cleaning',
        bash_command='cd /app && python silver_processor.py',
        doc_md="""
        **Silver** : Nettoyage et normalisation des données brutes.
        - Suppression HTML
        - Normalisation texte
        - Détection langue
        Sortie : data_lake/silver/
        """,
    )

    # ── Task 3 : Analytics (Gold Layer) ──────────────────────
    gold_processing = BashOperator(
        task_id='gold_analytics',
        bash_command='cd /app && python gold_processor.py',
        doc_md="""
        **Gold** : Génération des tables analytiques.
        - Tendances news
        - Top sujets / mots-clés
        - Articles par source / langue / date
        Sortie : data_lake/gold/
        """,
    )

    # ── Task 4 : Qualité des données ─────────────────────────
    quality_check = BashOperator(
        task_id='quality_check',
        bash_command='cd /app && python quality_checker.py',
        doc_md="""
        **Quality Check** : Contrôles qualité multi-dimensions.
        - Complétude (titres, dates, contenu)
        - Cohérence inter-couches
        - Validité (doublons, formats)
        """,
    )

    # ── Dépendances : pipeline séquentiel ────────────────────
    scraping >> silver_processing >> gold_processing >> quality_check


# ── DAG 2 : Qualité uniquement (quotidien) ───────────────────
with DAG(
    dag_id='news_quality_daily',
    default_args=default_args,
    description='Contrôle qualité quotidien des données',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['news', 'quality', 'iadata'],
) as dag_quality:

    daily_quality = BashOperator(
        task_id='daily_quality_report',
        bash_command='cd /app && python quality_checker.py',
    )
