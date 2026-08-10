"""
JOUR 4 / 10 — ETL/Airflow
DAG : TaskFlow API — syntaxe moderne avec décorateurs

Avant (Airflow 1.x) :
    def ma_fonction(**context): ...
    task = PythonOperator(task_id='ma_task', python_callable=ma_fonction)

Après (Airflow 2.0+ TaskFlow) :
    @task
    def ma_fonction(): ...
    result = ma_fonction()   # la task est créée et l'ordre est inféré

XCom automatique : le return d'un @task est automatiquement
transmis en argument à la task suivante — plus besoin de ti.xcom_pull()
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
import pandas as pd
import random

# ── Définir le DAG avec @dag ──────────────────────────────────
@dag(
    dag_id      = 'jour4_taskflow_intro',
    start_date  = datetime(2024, 1, 1),
    schedule    = '@daily',
    catchup     = False,
    tags        = ['jour4', 'taskflow'],
    default_args= {'owner': 'sung', 'retries': 1,
                   'retry_delay': timedelta(minutes=5)},
)
def pipeline_taskflow():
    """Pipeline ETL avec la syntaxe TaskFlow API."""

    # ── TASK 1 : @task remplace PythonOperator ────────────────
    @task
    def extraire(ds=None):
        """Extrait les données du jour."""
        random.seed(int(ds.replace('-', '')))
        produits = ['Laptop Pro','Smartphone X','Tablette Air','Écouteurs BT','Montre Smart']
        prix     = {'Laptop Pro':1200,'Smartphone X':650,'Tablette Air':450,
                    'Écouteurs BT':120,'Montre Smart':280}
        vendeurs = ['Alice','Karim','Lucie','Thomas','Nadia']

        rows = [{'produit': (p := random.choice(produits)),
                 'vendeur': random.choice(vendeurs),
                 'quantite': (q := random.randint(1, 10)),
                 'montant': q * prix[p],
                 'date': ds}
                for _ in range(random.randint(15, 25))]

        path = f'/tmp/raw_tf_{ds}.csv'
        pd.DataFrame(rows).to_csv(path, index=False)
        print(f"Extrait {len(rows)} lignes")
        return path    # ← transmis automatiquement à la task suivante

    # ── TASK 2 : reçoit le return de la task précédente ──────
    @task
    def transformer(path: str, ds=None):
        """Transforme les données brutes."""
        df = pd.read_csv(path)

        df['marge']        = (df['montant'] * 0.42).round(0).astype(int)
        df['taille_vente'] = pd.cut(df['montant'],
                                    bins=[0, 500, 2000, float('inf')],
                                    labels=['Petite','Moyenne','Grosse'])

        clean_path = f'/tmp/clean_tf_{ds}.csv'
        df.to_csv(clean_path, index=False)
        print(f"Transformé : {len(df)} lignes — CA: {df['montant'].sum():.0f}€")
        return clean_path

    # ── TASK 3 : reçoit le return de transformer ─────────────
    @task
    def charger(path: str, ds=None):
        """Charge les données transformées."""
        import sqlite3, os
        df = pd.read_csv(path)

        os.makedirs('/tmp/entrepot', exist_ok=True)
        conn = sqlite3.connect('/tmp/entrepot/ventes_tf.db')
        conn.execute("CREATE TABLE IF NOT EXISTS ventes ("
                     "date TEXT, produit TEXT, vendeur TEXT, "
                     "quantite INTEGER, montant REAL, marge REAL, "
                     "taille_vente TEXT)")
        conn.execute(f"DELETE FROM ventes WHERE date = '{ds}'")
        df.to_sql('ventes', conn, if_exists='append', index=False)
        conn.commit()

        count = conn.execute(f"SELECT COUNT(*) FROM ventes WHERE date='{ds}'"
                             ).fetchone()[0]
        conn.close()
        print(f"Chargé : {count} lignes")
        return count

    # ── TASK 4 : reçoit le count de charger ──────────────────
    @task
    def valider(nb_lignes: int):
        """Valide que le chargement a bien fonctionné."""
        if nb_lignes == 0:
            raise ValueError("Aucune ligne chargée — pipeline en échec")
        print(f"✓ Validation OK — {nb_lignes} lignes chargées")

    # ── Enchaînement : les dépendances sont inférées ──────────
    # Plus besoin de >> — l'ordre est déduit des arguments
    raw_path   = extraire()
    clean_path = transformer(raw_path)
    nb_lignes  = charger(clean_path)
    valider(nb_lignes)

# Instancier le DAG
dag_instance = pipeline_taskflow()
