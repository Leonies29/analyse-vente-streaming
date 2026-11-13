"""Lot A — Affiche les messages qui arrivent dans Kafka (vérification et démo).

    python -m scripts.check_kafka                   # nouveaux messages seulement
    python -m scripts.check_kafka --from-beginning  # depuis le début du topic
"""
import argparse
import json
import time

from kafka import KafkaConsumer

from config import settings


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--from-beginning", action="store_true")
    args = p.parse_args()

    consumer = KafkaConsumer(
        settings.KAFKA_TOPIC,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest" if args.from_beginning else "latest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id=None,
    )
    print(f"Écoute de « {settings.KAFKA_TOPIC} » sur {settings.KAFKA_BOOTSTRAP} (Ctrl+C pour arrêter)")
    n, t0 = 0, time.time()
    try:
        for m in consumer:
            n += 1
            v = m.value
            if n <= 10 or n % 1000 == 0:
                print(f"[{n}] {v['InvoiceDate']}  {v['InvoiceNo']:>8}  {v['Quantity']:>4} x "
                      f"{v['Description'][:35]:<35} {v['UnitPrice']:>7.2f} £  {v['Country']}")
            if n % 1000 == 0:
                print(f"     → {n / (time.time() - t0):.0f} messages/s")
    except KeyboardInterrupt:
        print(f"\n{n} messages reçus.")


if __name__ == "__main__":
    main()
