# Format des résultats (contrat entre Lot B et Lot C)

Spark (Lot B) réécrit ces fichiers CSV dans `output/` à chaque micro-batch
(via `foreachBatch`). Le dashboard (Lot C) les relit toutes les quelques secondes.
Pour éviter que le dashboard lise un fichier à moitié écrit : écrire dans `fichier.tmp`
puis renommer (`os.replace`).

| Fichier | Colonnes |
|---|---|
| `kpis.csv` (1 ligne) | ca_total, nb_commandes, panier_moyen, nb_clients, taux_annulation, derniere_maj |
| `ventes_par_fenetre.csv` | window_start, ca, nb_commandes, nb_articles |
| `top_produits.csv` | StockCode, Description, quantite, ca |
| `ca_par_pays.csv` | Country, ca, nb_commandes |
| `alertes.csv` | horodatage, niveau, message |

- Montants en livres sterling (£), `ca = Quantity x UnitPrice` hors annulations
- `taux_annulation` entre 0 et 1
- Dates au format `yyyy-MM-dd HH:mm:ss`
