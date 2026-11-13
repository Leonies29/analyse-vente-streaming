# Brouillon rapport — parties du Lot A
*(remplacer les [XXX] par les valeurs de `docs/nettoyage_stats.json` et du notebook)*

## 2. Données

### 2.1 Présentation du dataset
Le projet s'appuie sur le dataset *Online Retail* (Chen, 2015, UCI Machine Learning Repository). Il regroupe les transactions d'un site britannique de vente en ligne de cadeaux, entre le 1er décembre 2010 et le 9 décembre 2011. Chaque ligne correspond à un produit d'une facture et contient 8 colonnes : numéro de facture, code produit, description, quantité, date, prix unitaire, identifiant client et pays.

Le fichier brut compte [XXX] lignes, [XXX] factures, [XXX] produits et [XXX] pays.

### 2.2 Qualité des données
L'exploration (notebook `01_exploration.ipynb`) a mis en évidence plusieurs problèmes :
- **[XXX] doublons exacts** ;
- **[XXX] lignes sans identifiant client** (environ [XX] %) ;
- **des factures annulées**, dont le numéro commence par « C » et dont la quantité est négative ;
- **des quantités négatives hors annulation**, qui correspondent à des corrections de stock ;
- **des prix nuls**, liés à des cadeaux ou des ajustements ;
- **des codes qui ne sont pas des produits** : frais de port (POST, DOT), frais bancaires, ajustements manuels (M, D)…

### 2.3 Choix de nettoyage
| Règle | Lignes retirées | Justification |
|---|---|---|
| Doublons exacts | [XXX] | erreurs de saisie, ils gonfleraient le CA |
| Sans description | [XXX] | lignes inexploitables |
| Prix ≤ 0 | [XXX] | pas une vente réelle |
| Codes non-produits | [XXX] | frais et ajustements, pas des ventes de produits |
| Quantité négative hors annulation | [XXX] | corrections de stock |

Deux choix importants :
- **Les annulations sont conservées.** Elles servent à calculer le taux d'annulation, un indicateur utile pour le responsable. Elles sont exclues du chiffre d'affaires.
- **Les lignes sans client sont conservées.** Ce sont de vraies ventes : elles comptent dans le CA, mais pas dans le nombre de clients actifs.

Après nettoyage, il reste **[XXX] lignes**, dont [XXX] annulations.

### 2.4 Simulation du flux
Le dataset est historique. Pour simuler des ventes qui arrivent en continu, un producteur Python le relit **dans l'ordre chronologique** et envoie chaque ligne dans Kafka sous forme de message JSON, à un débit réglable (par défaut [200] transactions par seconde). Un an d'activité est ainsi rejoué en [XX] minutes.

La date d'origine de la transaction (`InvoiceDate`) est conservée dans le message. Spark l'utilise comme **temps de l'événement** (*event time*) : les indicateurs sont calculés selon le moment où la vente a eu lieu, et non selon le moment où le message est reçu.

## 3. Architecture

### 3.1 Vue d'ensemble
*(insérer le schéma « Architecture globale »)*

Le système est composé de quatre briques indépendantes :
1. **Le producteur** (Python) lit le fichier nettoyé et publie les ventes.
2. **Apache Kafka** reçoit les ventes dans le topic `transactions`.
3. **Spark Structured Streaming** lit ce topic, calcule les indicateurs et écrit les résultats.
4. **Le tableau de bord Streamlit** affiche ces résultats et se met à jour automatiquement.

### 3.2 Pourquoi Kafka ?
Spark pourrait lire directement des fichiers déposés dans un dossier. Kafka apporte pourtant plusieurs avantages, proches d'un cas réel :
- **Découplage** : la source des ventes et le traitement ne se connaissent pas. On peut changer l'un sans toucher à l'autre, ou brancher d'autres consommateurs (alertes, archivage…).
- **Absorption des pics** : si les ventes arrivent plus vite que Spark ne les traite, Kafka les garde en attente.
- **Rejeu** : les messages sont conservés, ce qui permet de relancer le traitement depuis le début, par exemple après une panne ou pour tester.
- **Ordre par facture** : le numéro de facture sert de clé, donc toutes les lignes d'une même facture arrivent dans la même partition, dans l'ordre.

### 3.3 Format des messages
```json
{"InvoiceNo": "536365", "StockCode": "85123A", "Description": "WHITE HANGING HEART T-LIGHT HOLDER",
 "Quantity": 6, "InvoiceDate": "2010-12-01 08:26:00", "UnitPrice": 2.55,
 "CustomerID": "17850", "Country": "United Kingdom"}
```

### 3.4 Déploiement
Kafka tourne dans un conteneur Docker (image officielle `apache/kafka`, en mode KRaft, donc sans Zookeeper), lancé avec `docker compose up -d`. Spark et le producteur s'exécutent en Python sur la machine. Cette configuration est simple à installer sur les postes des trois membres du groupe.
