"""
JOUR 2 / 10 — ETL/Airflow
DAG : FileSensor — attendre qu'un fichier soit disponible avant de continuer

Sensor = task qui attend qu'une condition soit remplie
FileSensor = attend qu'un fichier existe sur le système de fichiers
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
import pandas as pd

default_args = {
    'owner'      : 'sung',
    'start_date' : datetime(2024, 1, 1),
    'retries'    : 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    dag_id      = 'jour2_file_sensor',
    default_args= default_args,
    description = 'FileSensor — attendre un fichier avant de traiter',
    schedule    = '@daily',
    catchup     = False,
    tags        = ['jour2', 'sensor'],
)


# ── TASK 1 : Simuler l'arrivée du fichier source ─────────────
def creer_fichier_source(**context):
    """Simule un fichier déposé par un système externe."""
    import os
    os.makedirs('/tmp/inbox', exist_ok=True)
    path = f'/tmp/inbox/export_{context["ds"]}.csv'

    df = pd.DataFrame({
        'vendeur' : ['Alice','Karim','Lucie','Thomas','Nadia'],
        'ca'      : [45000, 38000, 22000, 15000, 31000],
        'objectif': [40000, 35000, 25000, 20000, 30000],
    })
    df.to_csv(path, index=False)
    print(f"Fichier source créé : {path}")

task_creer_source = PythonOperator(
    task_id='creer_fichier_source',
    python_callable=creer_fichier_source,
    dag=dag,
)


# ── TASK 2 : FileSensor — attendre le fichier ────────────────
# poke_interval : vérifie toutes les N secondes
# timeout       : abandonne après N secondes
# mode          : 'poke' (bloque) ou 'reschedule' (libère le worker)
sensor_fichier = FileSensor(
    task_id       = 'attendre_fichier_source',
    filepath      = '/tmp/inbox/export_{{ ds }}.csv',
    poke_interval = 30,      # vérifie toutes les 30 secondes
    timeout       = 300,     # abandonne après 5 minutes
    mode          = 'reschedule',
    dag           = dag,
)


# ── TASK 3 : Traiter le fichier une fois disponible ──────────
def traiter_fichier(**context):
    path = f'/tmp/inbox/export_{context["ds"]}.csv'
    df   = pd.read_csv(path)

    df['atteinte_obj'] = (df['ca'] / df['objectif'] * 100).round(1)
    df['statut']       = df['atteinte_obj'].apply(
        lambda x: '✓ Atteint' if x >= 100 else '✗ Non atteint'
    )

    print("=== Rapport Performance ===")
    print(df.to_string(index=False))
    print(f"\nCA Total    : {df['ca'].sum()}€")
    print(f"Obj Total   : {df['objectif'].sum()}€")
    print(f"Atteinte    : {(df['ca'].sum()/df['objectif'].sum()*100):.1f}%")

    output = f'/tmp/rapport_{context["ds"]}.csv'
    df.to_csv(output, index=False)
    return output

task_traiter = PythonOperator(
    task_id='traiter_fichier',
    python_callable=traiter_fichier,
    dag=dag,
)


# ── ORDRE ────────────────────────────────────────────────────
# Crée d'abord le fichier, puis le sensor le détecte, puis traitement
task_creer_source >> sensor_fichier >> task_traiter
