# Analyse des ventes en temps réel — Data Streaming

Projet ESME — Ameziane, Karl, Léonie.

Flux : CSV → producteur Python → Kafka → Spark Structured Streaming → output/ → dashboard Streamlit

## Installation
Prérequis : Python 3.10+, Java 17 (pour Spark), Docker.

```bash
python -m venv .venv
.venv/bin/activate       
pip install -r requirements.txt
```

Télécharger le dataset sur https://archive.ics.uci.edu/dataset/352/online+retail
et placer `Online Retail.xlsx` dans `data/raw/` sous le nom `online_retail.xlsx`.

## Lancement (4 terminaux, depuis la racine)
```bash
docker compose up -d                       # 1. Kafka
python -m producer.producer                # 2. envoi des transactions
python -m streaming.job                    # 3. traitement Spark
streamlit run dashboard/app.py             # 4. tableau de bord
```

## Tester le dashboard sans Kafka ni Spark
```bash
python -m scripts.generate_sample_output --live   # faux résultats qui évoluent
streamlit run dashboard/app.py                    # ouvre http://localhost:8501
```
Le format des fichiers lus par le dashboard est décrit dans `config/output_format.md`.

## Organisation
| Dossier | Lot | Contenu |
|---|---|---|
| `config/` | Tous | paramètres partagés + formats des messages Kafka et des résultats |
| `scripts/` | C | faux résultats pour tester le dashboard |
| `notebooks/`, `producer/` | A | nettoyage des données, envoi dans Kafka |
| `streaming/` | B | job Spark et indicateurs |
| `dashboard/` | C | application Streamlit |
| `docs/`, `presentation/` | Tous / C | rapport, schémas, diapos |

## Règles Git
- `main` doit toujours fonctionner : on n'y pousse pas directement
- Une branche par tâche : `lotA/producer`, `lotB/aggregations`, `lotC/dashboard`…
- Pull request relue par un autre membre avant fusion
- Les données et les résultats (`data/`, `output/`) ne sont jamais commités
