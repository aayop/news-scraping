# News Scraping Big Data Project

Projet EMSI Casablanca - Filiere IADATA - Architecture de donnees

Ce projet collecte des articles depuis plusieurs sites d'actualite, les stocke dans un Data Lake, les nettoie, construit des tables analytiques et affiche les resultats dans un dashboard.

## Objectif

L'objectif est de montrer une architecture data complete autour des articles de presse :

- collecte web scraping
- ingestion batch
- ingestion streaming sous forme d'evenements
- Data Lake
- architecture Medaillon Bronze, Silver, Gold
- transformations ETL
- qualite des donnees
- Data Warehouse MySQL avec XAMPP
- dashboard de visualisation
- orchestration avec Airflow
- execution possible avec Docker

## Architecture

```text
Sites d'actualite
        |
        v
Scraping Python
        |
        v
Bronze: donnees brutes JSON
        |
        v
Silver: nettoyage, normalisation, langue, deduplication
        |
        +--> Streaming: evenements article_published
        |
        v
Gold: tables analytiques JSON
        |
        v
Data Warehouse MySQL: news_warehouse
        |
        v
Dashboard HTML
```

## Sources scrapees

- Hespress
- Akhbarona
- Barlamane
- BBC News
- Al Jazeera
- Reuters

Les champs collectes sont principalement :

- titre
- auteur
- date de publication
- categorie
- contenu
- source
- URL
- date de scraping

## Structure du projet

```text
news_scraping_project/
|-- scrapers/
|   |-- main_scraper.py
|   |-- *_scraper.py
|   `-- requirements.txt
|-- dags/
|   `-- news_pipeline_dag.py
|-- data_lake/
|   |-- bronze/
|   |-- silver/
|   |-- streaming/
|   `-- gold/
|-- reports/
|-- dashboard.html
|-- pipeline.py
|-- silver_processor.py
|-- streaming_ingestion.py
|-- gold_processor.py
|-- warehouse_loader.py
|-- quality_checker.py
|-- storage.py
|-- Dockerfile
`-- docker-compose.yml
```

## Installation locale

Installer les dependances :

```bash
cd scrapers
pip install -r requirements.txt
cd ..
```

Sur Windows, si `python` ne marche pas, utiliser `py`.

## Lancer le pipeline

Pipeline complet :

```bash
py pipeline.py
```

Pipeline sans refaire le scraping :

```bash
py pipeline.py --skip-scraping
```

Lancer les etapes separement :

```bash
py scrapers/main_scraper.py
py silver_processor.py
py streaming_ingestion.py
py gold_processor.py
py warehouse_loader.py
py quality_checker.py
```

## Data Lake

Les donnees sont organisees en couches :

- `data_lake/bronze/` : articles bruts recuperes par les scrapers
- `data_lake/silver/` : articles nettoyes et dedupliques
- `data_lake/streaming/` : evenements `article_published`
- `data_lake/gold/` : tables analytiques

Le fichier `storage.py` permet d'utiliser soit le disque local, soit un stockage compatible S3 comme MinIO.

## Batch ingestion

Le batch est gere par Airflow dans :

```text
dags/news_pipeline_dag.py
```

Le DAG principal est planifie avec :

```text
@hourly
```

Donc le scraping et le traitement peuvent tourner automatiquement chaque heure.

## Streaming ingestion

Pour le streaming, chaque article nettoye est transforme en evenement :

```text
article_published
```

Les evenements sont sauvegardes dans :

```text
data_lake/streaming/
```

Ils sont aussi copies dans MySQL dans la table :

```text
news_warehouse.streaming_article_events
```

Cette partie simule un flux d'evenements. Dans une version production, on pourrait remplacer cette simulation par Kafka ou RabbitMQ.

## Data Warehouse avec XAMPP

Le Data Warehouse utilise MySQL/MariaDB de XAMPP.

1. Ouvrir XAMPP Control Panel
2. Demarrer MySQL
3. Lancer :

```bash
py warehouse_loader.py
```

La base creee est :

```text
news_warehouse
```

Tables principales :

- `fact_articles`
- `fact_articles_by_day`
- `fact_keywords`
- `streaming_article_events`
- `dim_source`
- `dim_category`
- `dim_language`
- `warehouse_summary`

Parametres par defaut :

```text
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=
MYSQL_DATABASE=news_warehouse
```

Pour verifier dans phpMyAdmin :

```text
http://localhost/phpmyadmin
```

## Dashboard

Le dashboard est dans :

```text
dashboard.html
```

Pour le lancer en local :

```bash
py -m http.server 8000
```

Puis ouvrir :

```text
http://localhost:8000/dashboard.html
```

Le dashboard affiche :

- nombre total d'articles
- repartition par source
- repartition par langue
- articles par date
- mots cles frequents
- derniers articles

## Qualite des donnees

Le fichier `quality_checker.py` verifie :

- titre manquant
- contenu trop court
- URL manquante
- format de date
- coherence entre Bronze, Silver et Gold
- presence des fichiers analytiques
- presence des evenements streaming

Les rapports sont sauvegardes dans :

```text
reports/
```

## Docker

Le projet contient :

- `Dockerfile`
- `docker-compose.yml`

Lancer avec Docker Compose :

```bash
docker-compose up --build
```

Lancer le dashboard avec Nginx :

```bash
docker-compose --profile dashboard up --build
```

Lancer le scheduler :

```bash
docker-compose --profile scheduler up
```

Si le pipeline tourne dans Docker et doit se connecter au MySQL de XAMPP sur Windows, utiliser :

```text
MYSQL_HOST=host.docker.internal
```

## Airflow

Le DAG Airflow suit cet ordre :

```text
Scraping -> Silver -> Streaming -> Gold -> Warehouse -> Quality
```

Cela montre l'orchestration du pipeline complet.

## Gouvernance et tracabilite

La tracabilite est assuree avec :

- `scraped_at`
- `processed_at`
- `loaded_at`
- rapports de pipeline
- rapports de qualite
- separation Bronze / Silver / Gold
- conservation des fichiers historiques dans le Data Lake

## Commandes utiles

Verifier la base MySQL :

```bash
C:\xampp\mysql\bin\mysql.exe -uroot -e "USE news_warehouse; SHOW TABLES;"
```

Verifier le nombre d'articles :

```bash
C:\xampp\mysql\bin\mysql.exe -uroot -e "USE news_warehouse; SELECT COUNT(*) FROM fact_articles;"
```

Verifier les evenements streaming :

```bash
C:\xampp\mysql\bin\mysql.exe -uroot -e "USE news_warehouse; SELECT COUNT(*) FROM streaming_article_events;"
```

## Remarque

Le projet utilise surtout Python pour rester simple a executer pendant la presentation. Les composants comme MinIO, Docker, Airflow et MySQL montrent comment la solution peut etre deployee dans une architecture data plus complete.
