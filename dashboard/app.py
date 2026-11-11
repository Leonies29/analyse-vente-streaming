"""Lot C — Tableau de bord Streamlit.

Lit les résultats écrits par Spark dans output/ (voir config/output_format.md)
et se rafraîchit automatiquement.

    streamlit run dashboard/app.py
"""
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

# Permet d'importer config/ quand on lance « streamlit run dashboard/app.py »
RACINE = Path(__file__).resolve().parents[1]
sys.path.append(str(RACINE))
from config import settings  # noqa: E402

OUT = RACINE / settings.OUTPUT_DIR
VIOLET = "#6C4CFF"

st.set_page_config(page_title="Ventes en temps réel", page_icon="📈", layout="wide")


def lire(cle: str) -> pd.DataFrame | None:
    chemin = OUT / settings.OUTPUT_FILES[cle]
    if not chemin.exists():
        return None
    try:
        return pd.read_csv(chemin)
    except (pd.errors.EmptyDataError, pd.errors.ParserError):
        return None


def euros(x: float) -> str:
    return f"{x:,.0f} £".replace(",", " ")


# ---------- Barre latérale ----------
with st.sidebar:
    st.header("Réglages")
    nb_top = st.slider("Nombre de produits dans le top", 5, 20, 10)
    critere = st.radio("Classer les produits par", ["quantite", "ca"],
                       format_func=lambda c: "Quantité vendue" if c == "quantite" else "Chiffre d'affaires")
    auto = st.toggle("Rafraîchissement automatique", value=True)
    st.caption(f"Mise à jour toutes les {settings.DASHBOARD_REFRESH_SECONDS} s")

st.title("📈 Suivi des ventes en temps réel")
st.caption("Online Retail — flux traité par Kafka et Spark Structured Streaming")


@st.fragment(run_every=settings.DASHBOARD_REFRESH_SECONDS if auto else None)
def tableau_de_bord():
    kpis = lire("kpis")
    if kpis is None or kpis.empty:
        st.info("En attente des premiers résultats dans `output/`… "
                "Lance le job Spark, ou `python -m scripts.generate_sample_output` pour tester.")
        return
    k = kpis.iloc[0]

    # ---------- Indicateurs clés ----------
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Chiffre d'affaires", euros(k["ca_total"]))
    c2.metric("Commandes", f"{int(k['nb_commandes']):,}".replace(",", " "))
    c3.metric("Panier moyen", f"{k['panier_moyen']:.2f} £")
    c4.metric("Clients actifs", f"{int(k['nb_clients']):,}".replace(",", " "))
    c5.metric("Taux d'annulation", f"{k['taux_annulation'] * 100:.1f} %")
    st.caption(f"Dernière mise à jour : {k['derniere_maj']}")

    # ---------- Alertes ----------
    alertes = lire("alertes")
    if alertes is not None and not alertes.empty:
        derniere = alertes.iloc[-1]
        icone = "🔺" if derniere["niveau"] == "pic" else "🔻"
        st.warning(f"{icone} {derniere['horodatage']} — {derniere['message']}")

    # ---------- Évolution des ventes ----------
    ventes = lire("ventes")
    if ventes is not None and not ventes.empty:
        ventes["window_start"] = pd.to_datetime(ventes["window_start"])
        fig = px.area(ventes, x="window_start", y="ca",
                      labels={"window_start": "", "ca": "CA (£)"},
                      color_discrete_sequence=[VIOLET])
        fig.update_layout(height=320, margin=dict(l=0, r=0, t=30, b=0),
                          title="Chiffre d'affaires par fenêtre de 10 minutes")
        st.plotly_chart(fig, width="stretch")

    gauche, droite = st.columns(2)

    # ---------- Top produits ----------
    produits = lire("produits")
    with gauche:
        st.subheader("Produits les plus vendus")
        if produits is not None and not produits.empty:
            top = produits.sort_values(critere, ascending=False).head(nb_top)
            fig = px.bar(top.iloc[::-1], x=critere, y="Description", orientation="h",
                         labels={"quantite": "Quantité", "ca": "CA (£)", "Description": ""},
                         color_discrete_sequence=[VIOLET])
            fig.update_layout(height=420, margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig, width="stretch")

    # ---------- Pays ----------
    pays = lire("pays")
    with droite:
        st.subheader("Chiffre d'affaires par pays")
        if pays is not None and not pays.empty:
            hors_uk = st.checkbox("Masquer le Royaume-Uni (marché principal)")
            p = pays[pays["Country"] != "United Kingdom"] if hors_uk else pays
            p = p.sort_values("ca", ascending=False).head(10)
            fig = px.bar(p.iloc[::-1], x="ca", y="Country", orientation="h",
                         labels={"ca": "CA (£)", "Country": ""},
                         color_discrete_sequence=[VIOLET])
            fig.update_layout(height=380, margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig, width="stretch")

    # ---------- Historique des alertes ----------
    if alertes is not None and not alertes.empty:
        with st.expander(f"Historique des alertes ({len(alertes)})"):
            st.dataframe(alertes.iloc[::-1], width="stretch", hide_index=True)


tableau_de_bord()
