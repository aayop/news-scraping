# ============================================================
# dags/news_pipeline_dag.py
# DAG Airflow - Orchestration du pipeline News
# EMSI Casablanca - IADATA 2025/2026
# ============================================================

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner": "iadata",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="news_pipeline_hourly",
    default_args=default_args,
    description="Pipeline complet: Scraping -> Bronze -> Silver -> Streaming -> Gold -> Warehouse -> Quality",
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["news", "scraping", "etl", "iadata"],
) as dag:

    scraping = BashOperator(
        task_id="scraping_bronze",
        bash_command="cd /app && python scrapers/main_scraper.py",
        doc_md="""
        **Scraping**: collecte les articles depuis Hespress, BBC, Al Jazeera,
        Reuters, Barlamane et Akhbarona. Sortie: `data_lake/bronze/*.json`.
        """,
    )

    silver_processing = BashOperator(
        task_id="silver_cleaning",
        bash_command="cd /app && python silver_processor.py",
        doc_md="""
        **Silver**: nettoyage, suppression HTML, normalisation texte,
        detection langue et deduplication. Sortie: `data_lake/silver/`.
        """,
    )

    streaming_ingestion = BashOperator(
        task_id="streaming_ingestion",
        bash_command="cd /app && python streaming_ingestion.py",
        doc_md="""
        **Streaming**: publie chaque article nettoye comme evenement
        `article_published`. Sorties: `data_lake/streaming/` et table
        MySQL `streaming_article_events`.
        """,
    )

    gold_processing = BashOperator(
        task_id="gold_analytics",
        bash_command="cd /app && python gold_processor.py",
        doc_md="""
        **Gold**: generation des tables analytiques: articles par source,
        categorie, langue, date, mots cles et statistiques globales.
        Sortie: `data_lake/gold/`.
        """,
    )

    warehouse_load = BashOperator(
        task_id="warehouse_load",
        bash_command="cd /app && python warehouse_loader.py",
        doc_md="""
        **Data Warehouse**: charge les tables Gold dans MySQL/MariaDB
        (`news_warehouse`) pour l'analyse decisionnelle.
        """,
    )

    quality_check = BashOperator(
        task_id="quality_check",
        bash_command="cd /app && python quality_checker.py",
        doc_md="""
        **Quality Check**: controle la completude, coherence, validite,
        fraicheur et unicite des donnees.
        """,
    )

    scraping >> silver_processing >> streaming_ingestion >> gold_processing >> warehouse_load >> quality_check


with DAG(
    dag_id="news_quality_daily",
    default_args=default_args,
    description="Controle qualite quotidien des donnees",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["news", "quality", "iadata"],
) as dag_quality:

    daily_quality = BashOperator(
        task_id="daily_quality_report",
        bash_command="cd /app && python quality_checker.py",
    )
