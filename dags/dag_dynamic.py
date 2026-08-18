"""
JOUR 6 / 10 — ETL/Airflow
DAGs Dynamiques — générer plusieurs DAGs depuis une config

Au lieu de créer 5 DAGs manuellement (un par région),
on génère tous les DAGs depuis une liste.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
import pandas as pd
import sqlite3
import random

# ── Config qui génère les DAGs ────────────────────────────────
# En production : lire depuis une Variable Airflow ou un fichier JSON
REGIONS_CONFIG = [
    {'region': 'Île-de-France', 'code': 'idf',  'objectif': 50000},
    {'region': 'PACA',          'code': 'paca', 'objectif': 35000},
    {'region': 'Grand Est',     'code': 'ge',   'objectif': 25000},
]


def creer_dag_region(region_cfg: dict):
    """Factory : crée un DAG pour une région donnée."""

    region   = region_cfg['region']
    code     = region_cfg['code']
    objectif = region_cfg['objectif']

    @dag(
        dag_id      = f'jour6_region_{code}',  # dag_id unique par région
        start_date  = datetime(2024, 1, 1),
        schedule    = '@daily',
        catchup     = False,
        tags        = ['jour6', 'dynamic', code],
        default_args= {'owner': 'sung', 'retries': 1,
                       'retry_delay': timedelta(minutes=3)},
    )
    def pipeline_region():
        f"""Pipeline ETL pour la région {region}."""

        @task
        def extraire_region(ds=None):
            random.seed(int(ds.replace('-', '')) + hash(region) % 1000)
            vendeurs = ['Alice','Karim','Lucie','Thomas','Nadia']
            produits = ['Laptop Pro','Smartphone X','Tablette Air']
            prix     = {'Laptop Pro':1200,'Smartphone X':650,'Tablette Air':450}

            rows = []
            for _ in range(random.randint(5, 20)):
                p   = random.choice(produits)
                qte = random.randint(1, 8)
                rows.append({
                    'date'    : ds,
                    'region'  : region,
                    'vendeur' : random.choice(vendeurs),
                    'produit' : p,
                    'montant' : qte * prix[p],
                })

            df   = pd.DataFrame(rows)
            path = f'/tmp/{code}_{ds}.csv'
            df.to_csv(path, index=False)
            print(f"[{region}] Extrait {len(df)} lignes — CA: {df['montant'].sum():.0f}€")
            return path

        @task
        def analyser_performance(path: str):
            df       = pd.read_csv(path)
            ca_total = df['montant'].sum()
            atteinte = ca_total / objectif * 100

            print(f"[{region}] CA: {ca_total:.0f}€ | Objectif: {objectif}€ | "
                  f"Atteinte: {atteinte:.1f}%")

            statut = ('✓ Atteint' if atteinte >= 100 else
                      '⚠ Proche'  if atteinte >= 80  else '✗ En dessous')
            print(f"[{region}] Statut : {statut}")
            return {'ca': ca_total, 'atteinte': atteinte, 'statut': statut}

        @task
        def sauvegarder(kpis: dict, ds=None):
            conn = sqlite3.connect(f'/tmp/region_{code}.db')
            conn.execute("""CREATE TABLE IF NOT EXISTS kpis
                            (date TEXT, ca REAL, atteinte REAL, statut TEXT)""")
            conn.execute(f"DELETE FROM kpis WHERE date='{ds}'")
            conn.execute(
                "INSERT INTO kpis VALUES (?,?,?,?)",
                (ds, kpis['ca'], kpis['atteinte'], kpis['statut'])
            )
            conn.commit()
            conn.close()
            print(f"[{region}] KPIs sauvegardés")

        raw  = extraire_region()
        kpis = analyser_performance(raw)
        sauvegarder(kpis)

    return pipeline_region()


# ── Générer tous les DAGs en une boucle ──────────────────────
# Airflow découvre ces DAGs automatiquement car ils sont
# assignés à des variables dans le scope global du fichier
for cfg in REGIONS_CONFIG:
    globals()[f'dag_region_{cfg["code"]}'] = creer_dag_region(cfg)

# Résultat : 3 DAGs créés automatiquement :
#   jour6_region_idf
#   jour6_region_paca
#   jour6_region_ge
