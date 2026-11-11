# Guide d'installation (à tester sur les 3 PC)

## Prérequis
| Outil | Vérifier avec | Remarque |
|---|---|---|
| Python 3.10 à 3.12 | `python --version` | |
| Java 17 | `java -version` | nécessaire pour Spark (Lot B) |
| Docker Desktop | `docker --version` | doit être **lancé** avant `docker compose` |
| Git | `git --version` | |

## Étapes
```bash
git clone <url-du-depot>
cd projet-data-streaming
python -m venv .venv
.venv\Scripts\activate            # Windows   (Mac/Linux : source .venv/bin/activate)
pip install -r requirements.txt

python -m scripts.download_data   # télécharge le dataset (~23 Mo)
python -m producer.prepare_data   # nettoie → data/processed/online_retail.csv
docker compose up -d              # démarre Kafka
```

## Vérifier que Kafka marche (2 terminaux)
```bash
# Terminal 1 : on écoute
python -m scripts.check_kafka
# Terminal 2 : on envoie 2 000 ventes
python -m producer.producer --limit 2000 --rate 500
```
Le terminal 1 doit afficher les ventes au fur et à mesure.

## Problèmes fréquents
- **« Kafka injoignable »** : Docker Desktop n'est pas lancé, ou Kafka démarre encore (attendre 15 s). `docker ps` doit montrer le conteneur `kafka`.
- **Port 9092 déjà utilisé** : un autre Kafka tourne. `docker compose down` puis relancer.
- **Repartir de zéro** (vider le topic) : `docker compose down` puis `docker compose up -d`.
- **Lecture Excel très lente** : normal la première fois (1–2 min), ensuite on n'utilise que le CSV nettoyé.
