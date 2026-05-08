
## Avant de commencer

- Demarrer MySQL dans XAMPP.
- Verifier que le dashboard local marche :

```text
http://127.0.0.1:8001/dashboard.html
```

Si le serveur n'est pas lance :

```bash
py -m http.server 8001
```

## Commande principale

Pour eviter d'attendre le scraping pendant la presentation :

```bash
py pipeline.py --skip-scraping
```

Normalement on doit voir :

```text
Silver Processing: COMPLETED
Streaming Events: COMPLETED
Gold Analytics: COMPLETED
Warehouse Load: COMPLETED
Quality Checks: COMPLETED
```

Dernier test :

```text
233 events
Quality score: 96/100
```

## Ordre de presentation

1. Montrer rapidement le README pour l'architecture.
2. Montrer les dossiers du Data Lake :

```text
data_lake/bronze
data_lake/silver
data_lake/streaming
data_lake/gold
```

3. Expliquer :

- Bronze = articles bruts
- Silver = articles nettoyes
- Streaming = evenements article_published
- Gold = tables analytiques

4. Montrer le fichier :

```text
data_lake/streaming/article_events_latest.json
```

Dire simplement :

```text
Ici chaque article nettoye devient un evenement.
Dans un vrai systeme temps reel, on peut remplacer cette partie par Kafka.
```

## MySQL / XAMPP

Ouvrir :

```text
http://localhost/phpmyadmin
```

Base :

```text
news_warehouse
```

Tables importantes :

```text
fact_articles
fact_articles_by_day
fact_keywords
streaming_article_events
dim_source
dim_category
dim_language
warehouse_summary
```

Verification rapide si besoin :

```bash
C:\xampp\mysql\bin\mysql.exe -uroot -e "USE news_warehouse; SELECT COUNT(*) FROM fact_articles;"
C:\xampp\mysql\bin\mysql.exe -uroot -e "USE news_warehouse; SELECT COUNT(*) FROM streaming_article_events;"
```

Remarque a retenir :

```text
fact_articles contient les articles actuels.
streaming_article_events garde l'historique des batches, donc le nombre peut etre plus grand.
```

## Dashboard

Ouvrir :

```text
http://127.0.0.1:8001/dashboard.html
```

Montrer :

- total articles
- repartition par source
- langues
- articles par date
- mots cles
- articles recents

## Airflow

Fichier :

```text
dags/news_pipeline_dag.py
```

Ordre :

```text
scraping_bronze -> silver_cleaning -> streaming_ingestion -> gold_analytics -> warehouse_load -> quality_check
```

Dire que le DAG est planifie chaque heure avec `@hourly`.

## Docker

Fichiers :

```text
Dockerfile
docker-compose.yml
```

Services a citer :

- news-intelligence
- minio
- dashboard-server
- scheduler

Si on demande comment Docker parle avec XAMPP :

```text
MYSQL_HOST=host.docker.internal
```

## Gouvernance

Montrer :

```text
reports/
```

Dire :

- les rapports gardent l'historique des executions
- les couches Bronze/Silver/Gold montrent le lignage
- les dates `scraped_at`, `processed_at`, `loaded_at` aident pour la tracabilite

## Phrase simple si on me demande de resumer

Le projet collecte des articles, les stocke dans un Data Lake, les nettoie, simule un flux streaming, cree des tables analytiques, charge MySQL comme Data Warehouse et affiche les resultats dans un dashboard.
