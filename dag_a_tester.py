"""
JOUR 9 / 10 — ETL/Airflow
DAG : dag_a_tester.py — Le DAG qu'on va tester

Ce fichier contient le pipeline de production.
Les tests se trouvent dans test_dag.py.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
import pandas as pd
import sqlite3
import os

DB_PATH = '/tmp/test_j9.db'


# ── Fonctions métier (testables indépendamment) ───────────────

def valider_donnees(df: pd.DataFrame) -> dict:
    """
    Valide la qualité des données.
    Retourne un rapport de validation.
    Fonction pure — facilement testable avec pytest.
    """
    erreurs = []

    if df.isnull().sum().sum() > 0:
        erreurs.append(f"Valeurs nulles : {df.isnull().sum().sum()}")
    if len(df) == 0:
        erreurs.append("DataFrame vide")
    if 'montant' in df.columns and (df['montant'] <= 0).any():
        erreurs.append(f"Montants négatifs : {(df['montant'] <= 0).sum()}")
    if 'vendeur' in df.columns:
        vendeurs_valides = {'Alice', 'Karim', 'Lucie', 'Thomas', 'Nadia'}
        inconnus = set(df['vendeur']) - vendeurs_valides
        if inconnus:
            erreurs.append(f"Vendeurs inconnus : {inconnus}")

    return {
        'valid'    : len(erreurs) == 0,
        'erreurs'  : erreurs,
        'nb_lignes': len(df),
    }


def transformer_donnees(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applique les transformations métier.
    Fonction pure — retourne un nouveau DataFrame.
    Facilement testable : input DataFrame → output DataFrame.
    """
    df = df.copy()
    df['marge']        = (df['montant'] * 0.42).round(2)
    df['taux_marge']   = 0.42
    df['taille_vente'] = pd.cut(
        df['montant'],
        bins=[0, 500, 2000, float('inf')],
        labels=['Petite', 'Moyenne', 'Grosse']
    ).astype(str)
    df['vendeur']      = df['vendeur'].str.title()
    return df


def calculer_kpis(df: pd.DataFrame) -> dict:
    """
    Calcule les KPIs à partir d'un DataFrame.
    Fonction pure — testable avec des DataFrames fixtures.
    """
    if len(df) == 0:
        return {'ca_total': 0, 'nb_ventes': 0, 'panier_moyen': 0,
                'top_produit': None, 'top_vendeur': None}

    return {
        'ca_total'    : round(float(df['montant'].sum()), 2),
        'nb_ventes'   : len(df),
        'panier_moyen': round(float(df['montant'].mean()), 2),
        'top_produit' : df.groupby('produit')['montant'].sum().idxmax(),
        'top_vendeur' : df.groupby('vendeur')['montant'].sum().idxmax(),
    }


# ── Le DAG ────────────────────────────────────────────────────

@dag(
    dag_id      = 'jour9_dag_a_tester',
    start_date  = datetime(2024, 1, 1),
    schedule    = '@daily',
    catchup     = False,
    tags        = ['jour9', 'tests'],
    default_args= {'owner': 'sung', 'retries': 1,
                   'retry_delay': timedelta(minutes=5)},
)
def pipeline_testable():
    """Pipeline dont les fonctions métier sont toutes testables."""

    @task
    def extraire(ds=None):
        import random
        random.seed(int(ds.replace('-', '')))
        produits = ['Laptop Pro','Smartphone X','Tablette Air','Écouteurs BT']
        prix     = {'Laptop Pro':1200,'Smartphone X':650,
                    'Tablette Air':450,'Écouteurs BT':120}
        vendeurs = ['Alice','Karim','Lucie','Thomas','Nadia']

        rows = [{'date':ds,'produit':p,'vendeur':random.choice(vendeurs),
                 'montant':(q:=random.randint(1,10))*prix[p],'quantite':q}
                for p in [random.choice(produits) for _ in range(20)]]

        df   = pd.DataFrame(rows)
        path = f'/tmp/raw_j9_{ds}.csv'
        df.to_csv(path, index=False)
        return path

    @task
    def valider(path: str):
        df      = pd.read_csv(path)
        rapport = valider_donnees(df)   # ← fonction pure testable

        if not rapport['valid']:
            raise ValueError(f"Validation échouée : {rapport['erreurs']}")

        print(f"✓ Validation OK — {rapport['nb_lignes']} lignes")
        return path

    @task
    def transformer(path: str, ds=None):
        df    = pd.read_csv(path)
        df_ok = transformer_donnees(df)   # ← fonction pure testable

        clean_path = f'/tmp/clean_j9_{ds}.csv'
        df_ok.to_csv(clean_path, index=False)
        return clean_path

    @task
    def charger(path: str, ds=None):
        df   = pd.read_csv(path)
        kpis = calculer_kpis(df)   # ← fonction pure testable

        conn = sqlite3.connect(DB_PATH)
        conn.execute("""CREATE TABLE IF NOT EXISTS ventes
                        (date TEXT, produit TEXT, vendeur TEXT,
                         montant REAL, quantite INTEGER,
                         marge REAL, taille_vente TEXT)""")
        conn.execute(f"DELETE FROM ventes WHERE date='{ds}'")
        df.to_sql('ventes', conn, if_exists='append', index=False)
        conn.commit(); conn.close()

        print(f"KPIs : {kpis}")
        return kpis

    raw   = extraire()
    valid = valider(raw)
    clean = transformer(valid)
    charger(clean)

dag_instance = pipeline_testable()
