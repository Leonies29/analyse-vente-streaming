"""Lot A — Nettoie le dataset brut et produit data/processed/online_retail.csv,
trié par date, prêt à être rejoué dans Kafka.

    python -m producer.prepare_data

Règles de nettoyage (à reprendre dans le rapport, partie 2) :
 1. Doublons exacts supprimés
 2. Lignes sans description supprimées
 3. Prix unitaire <= 0 supprimés (cadeaux, corrections de stock)
 4. Codes qui ne sont pas des produits supprimés (frais de port, frais bancaires, ajustements…)
 5. Quantités négatives hors annulation supprimées (corrections de stock)
 6. Annulations (InvoiceNo commençant par « C ») CONSERVÉES pour calculer le taux d'annulation
 7. CustomerID manquant conservé (vide) : la vente compte dans le CA mais pas dans les clients
"""
import json
from pathlib import Path

import pandas as pd

from config import settings

COLONNES = ["InvoiceNo", "StockCode", "Description", "Quantity",
            "InvoiceDate", "UnitPrice", "CustomerID", "Country"]

# Codes « techniques » présents dans le dataset, qui ne sont pas des produits
CODES_NON_PRODUITS = {"POST", "DOT", "M", "D", "C2", "S", "B", "BANK CHARGES",
                      "AMAZONFEE", "CRUK", "PADS", "M"}

STATS_PATH = Path("docs/nettoyage_stats.json")


def charger_brut(chemin: str) -> pd.DataFrame:
    types = {"InvoiceNo": str, "StockCode": str, "Description": str, "Country": str}
    if chemin.endswith(".csv"):
        df = pd.read_csv(chemin, dtype=types, encoding="latin-1")
    else:
        print("Lecture du fichier Excel (1 à 2 minutes)…")
        df = pd.read_excel(chemin, dtype=types)
    return df[COLONNES]


def nettoyer(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    stats = {"lignes_brutes": len(df)}

    def etape(nom: str, masque_a_garder) -> None:
        nonlocal df
        avant = len(df)
        df = df[masque_a_garder]
        stats[nom] = avant - len(df)

    df = df.copy()
    df["InvoiceNo"] = df["InvoiceNo"].astype(str).str.strip()
    df["StockCode"] = df["StockCode"].astype(str).str.strip().str.upper()
    df["Description"] = df["Description"].astype("string").str.strip()
    df["Country"] = df["Country"].astype(str).str.strip()

    avant = len(df)
    df = df.drop_duplicates()
    stats["doublons_supprimes"] = avant - len(df)

    etape("sans_description", df["Description"].notna() & (df["Description"] != ""))
    etape("prix_nul_ou_negatif", df["UnitPrice"] > 0)
    etape("codes_non_produits", ~df["StockCode"].isin(CODES_NON_PRODUITS))

    est_annulation = df["InvoiceNo"].str.startswith("C")
    etape("quantite_negative_hors_annulation", est_annulation | (df["Quantity"] > 0))

    # Mise en forme pour Kafka (voir config/message_format.md)
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
    df = df.sort_values("InvoiceDate", kind="stable")
    df["InvoiceDate"] = df["InvoiceDate"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df["CustomerID"] = (pd.to_numeric(df["CustomerID"], errors="coerce")
                        .astype("Int64").astype("string"))
    df["Quantity"] = df["Quantity"].astype(int)
    df["UnitPrice"] = df["UnitPrice"].astype(float).round(2)

    est_annulation = df["InvoiceNo"].str.startswith("C")
    stats["lignes_finales"] = len(df)
    stats["dont_annulations"] = int(est_annulation.sum())
    stats["dont_sans_client"] = int(df["CustomerID"].isna().sum())
    stats["nb_factures"] = int(df["InvoiceNo"].nunique())
    stats["nb_produits"] = int(df["StockCode"].nunique())
    stats["nb_pays"] = int(df["Country"].nunique())
    stats["periode"] = [df["InvoiceDate"].iloc[0], df["InvoiceDate"].iloc[-1]]
    return df, stats


def main() -> None:
    brut = settings.RAW_DATA
    if not Path(brut).exists():
        raise SystemExit(f"{brut} introuvable : lance d'abord « python -m scripts.download_data »")
    df, stats = nettoyer(charger_brut(brut))

    sortie = Path(settings.CLEAN_DATA)
    sortie.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(sortie, index=False)

    STATS_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATS_PATH.write_text(json.dumps(stats, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"\nFichier nettoyé : {sortie}")
    for cle, val in stats.items():
        print(f"  {cle:<35} {val}")
    print(f"\nStatistiques enregistrées dans {STATS_PATH} (pour le rapport)")


if __name__ == "__main__":
    main()
