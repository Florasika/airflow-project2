"""
JOUR 1 / 10 — ETL/Airflow
DAG : ETL Simple — Extract → Transform → Load
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import os

default_args = {
    'owner'           : 'sung',
    'depends_on_past' : False,
    'start_date'      : datetime(2024, 1, 1),
    'retries'         : 1,
    'retry_delay'     : timedelta(minutes=2),
}

dag = DAG(
    dag_id      = 'jour1_etl_simple',
    default_args= default_args,
    description = 'ETL basique Extract→Transform→Load',
    schedule    = '@daily',
    catchup     = False,
    tags        = ['jour1', 'etl'],
)

# ── EXTRACT ───────────────────────────────────────────────────
def extract(**context):
    import random
    random.seed(42)

    produits   = ['Laptop Pro','Smartphone X','Tablette Air','Écouteurs BT','Montre Smart']
    categories = {'Laptop Pro':'Informatique','Smartphone X':'Mobile',
                  'Tablette Air':'Informatique','Écouteurs BT':'Audio','Montre Smart':'Wearable'}
    prix       = {'Laptop Pro':1200,'Smartphone X':650,'Tablette Air':450,
                  'Écouteurs BT':120,'Montre Smart':280}

    rows = []
    for _ in range(20):
        p   = random.choice(produits)
        qte = random.randint(1, 10)
        rows.append({
            'produit'  : p,
            'categorie': categories[p],
            'quantite' : qte,
            'prix'     : prix[p],
            'montant'  : qte * prix[p],
            'vendeur'  : random.choice(['Alice','Karim','Lucie']),
            'date'     : context['ds'],
        })

    df       = pd.DataFrame(rows)
    raw_path = f'/tmp/raw_{context["ds"]}.csv'
    df.to_csv(raw_path, index=False)
    print(f"EXTRACT ✓ — {len(df)} lignes extraites → {raw_path}")
    return raw_path

task_extract = PythonOperator(
    task_id='extract', python_callable=extract, dag=dag)

# ── TRANSFORM ─────────────────────────────────────────────────
def transform(**context):
    ti       = context['ti']
    raw_path = ti.xcom_pull(task_ids='extract')
    df       = pd.read_csv(raw_path)

    df['marge']       = (df['montant'] * 0.4).round(0).astype(int)
    df['taille_vente']= pd.cut(
        df['montant'],
        bins=[0, 500, 2000, float('inf')],
        labels=['Petite','Moyenne','Grosse']
    )
    df['vendeur'] = df['vendeur'].str.title()

    clean_path = f'/tmp/clean_{context["ds"]}.csv'
    df.to_csv(clean_path, index=False)
    print(f"TRANSFORM ✓ — {len(df)} lignes → {clean_path}")
    print(f"  CA Total : {df['montant'].sum()}€")
    return clean_path

task_transform = PythonOperator(
    task_id='transform', python_callable=transform, dag=dag)

# ── LOAD ──────────────────────────────────────────────────────
def load(**context):
    ti         = context['ti']
    clean_path = ti.xcom_pull(task_ids='transform')
    df         = pd.read_csv(clean_path)

    os.makedirs('/tmp/entrepot', exist_ok=True)
    dest_path = f'/tmp/entrepot/ventes_{context["ds"]}.csv'
    df.to_csv(dest_path, index=False)
    print(f"LOAD ✓ — {len(df)} lignes → {dest_path}")
    print(f"  CA Total chargé : {df['montant'].sum()}€")

task_load = PythonOperator(
    task_id='load', python_callable=load, dag=dag)

# ── ORDRE ─────────────────────────────────────────────────────
task_extract >> task_transform >> task_load
