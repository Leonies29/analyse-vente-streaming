"""Lot A — Rejoue le dataset nettoyé dans Kafka, dans l'ordre chronologique,
pour simuler des ventes qui arrivent en continu.

    python -m producer.producer                    # vitesse par défaut (settings.EVENTS_PER_SECOND)
    python -m producer.producer --rate 1000        # plus rapide
    python -m producer.producer --limit 5000       # n'envoie que 5 000 lignes (tests)
    python -m producer.producer --dry-run          # affiche les messages sans Kafka

Chaque ligne du CSV devient un message JSON (format : config/message_format.md),
avec InvoiceNo comme clé : toutes les lignes d'une facture vont dans la même partition.
InvoiceDate garde la date d'origine (2010-2011) : Spark s'en sert comme temps
de l'événement pour ses fenêtres.
"""
import argparse
import json
import time

import pandas as pd

from config import settings


def lire_messages(chemin: str, limite: int | None):
    df = pd.read_csv(chemin, dtype={"InvoiceNo": str, "StockCode": str, "CustomerID": str},
                     keep_default_na=False, na_values={"CustomerID": [""]})
    if limite:
        df = df.head(limite)
    df["CustomerID"] = df["CustomerID"].astype(object).where(df["CustomerID"].notna(), None)
    return df.to_dict(orient="records")


def creer_producteur():
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable
    try:
        return KafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            linger_ms=20,
            acks=1,
        )
    except NoBrokersAvailable:
        raise SystemExit(f"Kafka injoignable sur {settings.KAFKA_BOOTSTRAP}. "
                         "As-tu lancé « docker compose up -d » ?")


def main() -> None:
    p = argparse.ArgumentParser(description="Rejoue les ventes dans Kafka")
    p.add_argument("--rate", type=float, default=settings.EVENTS_PER_SECOND,
                   help="transactions envoyées par seconde")
    p.add_argument("--limit", type=int, default=None, help="nombre max de lignes")
    p.add_argument("--dry-run", action="store_true", help="affiche au lieu d'envoyer")
    args = p.parse_args()

    messages = lire_messages(settings.CLEAN_DATA, args.limit)
    total = len(messages)
    print(f"{total} transactions à envoyer sur « {settings.KAFKA_TOPIC} » à {args.rate:.0f}/s "
          f"(durée estimée : {total / args.rate / 60:.1f} min)")

    producteur = None if args.dry_run else creer_producteur()
    debut = time.time()
    envoyes = 0
    try:
        for msg in messages:
            if args.dry_run:
                if envoyes < 5:
                    print(json.dumps(msg, ensure_ascii=False))
            else:
                producteur.send(settings.KAFKA_TOPIC, key=msg["InvoiceNo"], value=msg)
            envoyes += 1

            # Régulation du débit : on attend si on est en avance sur le rythme voulu
            retard = envoyes / args.rate - (time.time() - debut)
            if retard > 0:
                time.sleep(retard)

            if envoyes % 5000 == 0 or envoyes == total:
                ecoule = time.time() - debut
                print(f"  {envoyes}/{total} envoyés — date simulée {msg['InvoiceDate']} "
                      f"— {envoyes / ecoule:.0f} msg/s")
    except KeyboardInterrupt:
        print("\nArrêt demandé.")
    finally:
        if producteur:
            producteur.flush()
            producteur.close()
    print(f"Terminé : {envoyes} messages en {time.time() - debut:.1f} s")


if __name__ == "__main__":
    main()
