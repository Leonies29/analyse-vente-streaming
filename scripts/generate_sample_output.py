"""Génère de faux résultats dans output/ pour développer le dashboard
sans attendre Kafka et Spark.

    python -m scripts.generate_sample_output           # une fois
    python -m scripts.generate_sample_output --live    # simule un flux (Ctrl+C pour arrêter)
"""
import argparse
import os
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from config import settings

OUT = Path(settings.OUTPUT_DIR)

PRODUITS = [
    ("85123A", "WHITE HANGING HEART T-LIGHT HOLDER"),
    ("71053", "WHITE METAL LANTERN"),
    ("84879", "ASSORTED COLOUR BIRD ORNAMENT"),
    ("22423", "REGENCY CAKESTAND 3 TIER"),
    ("85099B", "JUMBO BAG RED RETROSPOT"),
    ("47566", "PARTY BUNTING"),
    ("20725", "LUNCH BAG RED RETROSPOT"),
    ("22720", "SET OF 3 CAKE TINS PANTRY DESIGN"),
    ("21212", "PACK OF 72 RETROSPOT CAKE CASES"),
    ("23084", "RABBIT NIGHT LIGHT"),
    ("22086", "PAPER CHAIN KIT 50'S CHRISTMAS"),
    ("84077", "WORLD WAR 2 GLIDERS ASSTD DESIGNS"),
]
PAYS = ["United Kingdom", "Germany", "France", "EIRE", "Spain",
        "Netherlands", "Belgium", "Switzerland", "Portugal", "Australia"]


def ecrire(df: pd.DataFrame, cle: str) -> None:
    """Écriture atomique : fichier temporaire puis renommage."""
    chemin = OUT / settings.OUTPUT_FILES[cle]
    tmp = chemin.with_suffix(".tmp")
    df.to_csv(tmp, index=False)
    os.replace(tmp, chemin)


def generer(debut: datetime, nb_fenetres: int) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    fenetres = [debut + timedelta(minutes=10 * i) for i in range(nb_fenetres)]
    ventes = pd.DataFrame({
        "window_start": [f.strftime("%Y-%m-%d %H:%M:%S") for f in fenetres],
        "ca": [round(random.uniform(800, 4000), 2) for _ in fenetres],
        "nb_commandes": [random.randint(5, 40) for _ in fenetres],
        "nb_articles": [random.randint(200, 2500) for _ in fenetres],
    })
    ecrire(ventes, "ventes")

    produits = pd.DataFrame([
        {"StockCode": c, "Description": d,
         "quantite": random.randint(50, 900) * nb_fenetres // 10 + 1,
         "ca": round(random.uniform(100, 2500) * nb_fenetres / 10, 2)}
        for c, d in PRODUITS
    ]).sort_values("quantite", ascending=False)
    ecrire(produits, "produits")

    poids = [80] + [random.uniform(1, 6) for _ in PAYS[1:]]
    total = ventes["ca"].sum()
    pays = pd.DataFrame({
        "Country": PAYS,
        "ca": [round(total * w / sum(poids), 2) for w in poids],
        "nb_commandes": [max(1, int(ventes["nb_commandes"].sum() * w / sum(poids))) for w in poids],
    })
    ecrire(pays, "pays")

    nb_cmd = int(ventes["nb_commandes"].sum())
    kpis = pd.DataFrame([{
        "ca_total": round(total, 2),
        "nb_commandes": nb_cmd,
        "panier_moyen": round(total / nb_cmd, 2),
        "nb_clients": int(nb_cmd * 0.7),
        "taux_annulation": round(random.uniform(0.01, 0.04), 4),
        "derniere_maj": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }])
    ecrire(kpis, "kpis")

    alertes = []
    for f, ca in zip(ventes["window_start"], ventes["ca"]):
        if ca > 3700:
            alertes.append({"horodatage": f, "niveau": "pic", "message": f"Pic de ventes : {ca:.0f} £ en 10 min"})
        elif ca < 900:
            alertes.append({"horodatage": f, "niveau": "chute", "message": f"Chute des ventes : {ca:.0f} £ en 10 min"})
    ecrire(pd.DataFrame(alertes, columns=["horodatage", "niveau", "message"]), "alertes")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true", help="ajoute une fenêtre toutes les 3 secondes")
    args = parser.parse_args()
    debut = datetime(2010, 12, 1, 8, 0)
    if not args.live:
        generer(debut, 60)
        print(f"Faux résultats écrits dans {OUT}/")
    else:
        n = 5
        while True:
            generer(debut, n)
            print(f"{n} fenêtres écrites")
            n += 1
            time.sleep(3)
