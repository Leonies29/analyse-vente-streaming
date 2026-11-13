"""Lot A — Télécharge le dataset Online Retail depuis l'UCI dans data/raw/.

    python -m scripts.download_data
"""
import io
import urllib.request
import zipfile
from pathlib import Path

from config import settings

URL = "https://archive.ics.uci.edu/static/public/352/online+retail.zip"


def main() -> None:
    cible = Path(settings.RAW_DATA)
    if cible.exists():
        print(f"Déjà présent : {cible}")
        return
    cible.parent.mkdir(parents=True, exist_ok=True)
    print(f"Téléchargement depuis {URL} …")
    with urllib.request.urlopen(URL) as rep:
        contenu = rep.read()
    with zipfile.ZipFile(io.BytesIO(contenu)) as z:
        nom = next(n for n in z.namelist() if n.lower().endswith(".xlsx"))
        cible.write_bytes(z.read(nom))
    print(f"OK : {cible} ({cible.stat().st_size / 1e6:.1f} Mo)")


if __name__ == "__main__":
    main()
