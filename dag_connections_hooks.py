"""
JOUR 3 / 10 — ETL/Airflow
DAG : Connexions & Hooks

Concepts :
    Connection = credentials stockés dans Airflow (UI ou CLI)
    Hook       = interface Python qui utilise une Connection
    Operator   = utilise un Hook pour interagir avec un système

Ce DAG utilise SQLiteHook (pas besoin de serveur externe)
pour démontrer le pattern Connection → Hook → Operator.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
import pandas as pd
import sqlite3
import os

default_args = {
    'owner'           : 'sung',
    'depends_on_past' : False,
    'start_date'      : datetime(2024, 1, 1),
    'retries'         : 1,
    'retry_delay'     : timedelta(minutes=2),
}

dag = DAG(
    dag_id      = 'jour3_connections_hooks',
    default_args= default_args,
    description = 'Connexions, Hooks, SQLite, HTTP',
    schedule    = '@daily',
    catchup     = False,
    tags        = ['jour3', 'hooks', 'connexions'],
)

# Chemin de la base SQLite (utilisé comme "entrepôt" local)
DB_PATH = '/tmp/entrepot_ventes.db'


# ── TASK 1 : Créer et initialiser la base SQLite ─────────────
def init_base_sqlite(**context):
    """
    Crée la base SQLite et la table ventes si elles n'existent pas.
    En production : cette étape serait remplacée par une connexion
    PostgreSQL/MySQL via un Hook configuré dans l'UI Airflow.
    """
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS ventes (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            date       TEXT NOT NULL,
            produit    TEXT NOT NULL,
            categorie  TEXT NOT NULL,
            vendeur    TEXT NOT NULL,
            region     TEXT NOT NULL,
            quantite   INTEGER NOT NULL,
            montant    REAL NOT NULL,
            charge_le  TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()
    print(f"Base SQLite initialisée : {DB_PATH}")

task_init_db = PythonOperator(
    task_id='init_base_sqlite',
    python_callable=init_base_sqlite,
    dag=dag,
)


# ── TASK 2 : Extraire des données (simulé) ───────────────────
def extraire_donnees(**context):
    """
    Simule l'extraction depuis une API ou un fichier source.
    En production : HttpHook, S3Hook, SFTPHook...
    """
    import random
    random.seed(int(context['ds'].replace('-', '')))

    produits   = ['Laptop Pro','Smartphone X','Tablette Air','Écouteurs BT','Montre Smart']
    categories = {'Laptop Pro':'Informatique','Smartphone X':'Mobile',
                  'Tablette Air':'Informatique','Écouteurs BT':'Audio','Montre Smart':'Wearable'}
    prix       = {'Laptop Pro':1200,'Smartphone X':650,'Tablette Air':450,
                  'Écouteurs BT':120,'Montre Smart':280}
    vendeurs   = ['Alice','Karim','Lucie','Thomas','Nadia']
    regions    = ['Île-de-France','PACA','Grand Est','Auvergne-Rhône-Alpes','Occitanie']

    rows = []
    for _ in range(random.randint(15, 30)):
        p   = random.choice(produits)
        qte = random.randint(1, 10)
        rows.append({
            'date'     : context['ds'],
            'produit'  : p,
            'categorie': categories[p],
            'vendeur'  : random.choice(vendeurs),
            'region'   : random.choice(regions),
            'quantite' : qte,
            'montant'  : qte * prix[p],
        })

    df   = pd.DataFrame(rows)
    path = f'/tmp/raw_{context["ds"]}.csv'
    df.to_csv(path, index=False)
    print(f"Extrait {len(df)} lignes — CA: {df['montant'].sum():.0f}€")
    return path

task_extraire = PythonOperator(
    task_id='extraire_donnees',
    python_callable=extraire_donnees,
    dag=dag,
)


# ── TASK 3 : Charger dans SQLite via Hook ────────────────────
def charger_sqlite(**context):
    """
    Charge les données dans SQLite.
    Pattern identique avec PostgresHook, MySqlHook, etc.
    """
    ti   = context['ti']
    path = ti.xcom_pull(task_ids='extraire_donnees')
    df   = pd.read_csv(path)
    df['charge_le'] = datetime.now().isoformat()

    # Connexion SQLite directe
    # Avec PostgreSQL ce serait :
    #   from airflow.providers.postgres.hooks.postgres import PostgresHook
    #   hook = PostgresHook(postgres_conn_id='postgres_prod')
    #   conn = hook.get_conn()
    conn = sqlite3.connect(DB_PATH)

    # Supprimer les données du jour pour éviter les doublons
    conn.execute(f"DELETE FROM ventes WHERE date = '{context['ds']}'")

    df.to_sql('ventes', conn, if_exists='append', index=False)
    conn.commit()

    # Vérification
    count = conn.execute(
        f"SELECT COUNT(*) FROM ventes WHERE date = '{context['ds']}'"
    ).fetchone()[0]
    conn.close()

    print(f"Chargé : {count} lignes pour le {context['ds']}")
    print(f"CA Total chargé : {df['montant'].sum():.0f}€")

task_charger = PythonOperator(
    task_id='charger_sqlite',
    python_callable=charger_sqlite,
    dag=dag,
)


# ── TASK 4 : Requêter la base et générer un rapport ──────────
def generer_rapport(**context):
    """Lit depuis SQLite et génère un rapport agrégé."""
    conn = sqlite3.connect(DB_PATH)

    # Rapport par vendeur
    df_vendeur = pd.read_sql("""
        SELECT
            vendeur,
            COUNT(*)            AS nb_ventes,
            SUM(quantite)       AS unites_vendues,
            ROUND(SUM(montant)) AS ca_total,
            ROUND(AVG(montant)) AS panier_moyen
        FROM ventes
        GROUP BY vendeur
        ORDER BY ca_total DESC
    """, conn)

    # Rapport par catégorie
    df_categorie = pd.read_sql("""
        SELECT
            categorie,
            ROUND(SUM(montant)) AS ca_total,
            ROUND(100.0 * SUM(montant) /
                (SELECT SUM(montant) FROM ventes), 1) AS part_pct
        FROM ventes
        GROUP BY categorie
        ORDER BY ca_total DESC
    """, conn)

    conn.close()

    print("=== Rapport par Vendeur ===")
    print(df_vendeur.to_string(index=False))
    print("\n=== Rapport par Catégorie ===")
    print(df_categorie.to_string(index=False))

    # Sauvegarder les rapports
    df_vendeur.to_csv(f'/tmp/rapport_vendeur_{context["ds"]}.csv', index=False)
    df_categorie.to_csv(f'/tmp/rapport_categorie_{context["ds"]}.csv', index=False)
    print("\nRapports sauvegardés dans /tmp/")

task_rapport = PythonOperator(
    task_id='generer_rapport',
    python_callable=generer_rapport,
    dag=dag,
)


# ── TASK 5 : Vérification finale ─────────────────────────────
task_verif = BashOperator(
    task_id      = 'verification_finale',
    bash_command = (
        'echo "Pipeline Jour 3 terminé — {{ ds }}" && '
        f'sqlite3 {DB_PATH} "SELECT COUNT(*) || \' lignes totales\' FROM ventes;"'
    ),
    dag=dag,
)


# ── ORDRE ────────────────────────────────────────────────────
task_init_db >> task_extraire >> task_charger >> task_rapport >> task_verif
