"""Paramètres partagés par les 3 lots. Ne pas dupliquer ces valeurs ailleurs."""

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "transactions"

RAW_DATA = "data/raw/online_retail.xlsx"        # fichier téléchargé depuis l'UCI
CLEAN_DATA = "data/processed/online_retail.csv" # fichier nettoyé (Lot A)

OUTPUT_DIR = "output"                # résultats écrits par Spark, lus par le dashboard
CHECKPOINT_DIR = "output/checkpoints"

# Vitesse du rejeu : nb de transactions envoyées par seconde
EVENTS_PER_SECOND = 200

# Spark : connecteur Kafka (doit correspondre à la version de pyspark)
SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

# Fichiers de résultats (voir config/output_format.md)
OUTPUT_FILES = {
    "kpis": "kpis.csv",
    "ventes": "ventes_par_fenetre.csv",
    "produits": "top_produits.csv",
    "pays": "ca_par_pays.csv",
    "alertes": "alertes.csv",
}

DASHBOARD_REFRESH_SECONDS = 5
