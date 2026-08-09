"""
JOUR 1 / 10 — ETL/Airflow
DAG : Hello World — Introduction aux concepts de base

Concepts :
    DAG      → Directed Acyclic Graph (le pipeline)
    Task     → une étape du pipeline
    Operator → le type de tâche (Bash, Python, SQL...)
    Schedule → la fréquence d'exécution
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import os

# ── Paramètres par défaut du DAG ─────────────────────────────
default_args = {
    'owner'           : 'sung',
    'depends_on_past' : False,
    'start_date'      : datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry'  : False,
    'retries'         : 1,
    'retry_delay'     : timedelta(minutes=5),
}

# ── Définition du DAG ─────────────────────────────────────────
dag = DAG(
    dag_id      = 'jour1_hello_world',
    default_args= default_args,
    description = 'Mon premier DAG Airflow — Jour 1/10',
    schedule    = '@daily',
    catchup     = False,
    tags        = ['jour1', 'debutant'],
)

# ── TASK 1 : BashOperator ─────────────────────────────────────
task_bash = BashOperator(
    task_id      = 'afficher_message',
    bash_command = 'echo "Airflow fonctionne ! Date : {{ ds }}"',
    dag          = dag,
)

# ── TASK 2 : PythonOperator ───────────────────────────────────
def creer_fichier_test(**context):
    date_exec = context['ds']
    print(f"Exécution du DAG pour la date : {date_exec}")

    df = pd.DataFrame({
        'produit'  : ['Laptop Pro', 'Smartphone X', 'Tablette Air'],
        'quantite' : [5, 12, 3],
        'prix'     : [1200, 650, 450],
        'montant'  : [6000, 7800, 1350],
        'date'     : [date_exec] * 3,
    })

    output_path = f'/tmp/ventes_{date_exec}.csv'
    df.to_csv(output_path, index=False)
    print(f"Fichier créé : {output_path}")
    return output_path

task_python = PythonOperator(
    task_id         = 'creer_fichier_csv',
    python_callable = creer_fichier_test,
    dag             = dag,
)

# ── TASK 3 : Vérifier le fichier ─────────────────────────────
def verifier_fichier(**context):
    ti     = context['ti']
    chemin = ti.xcom_pull(task_ids='creer_fichier_csv')

    if chemin and os.path.exists(chemin):
        df = pd.read_csv(chemin)
        print(f"✓ Fichier trouvé : {chemin}")
        print(f"✓ Lignes        : {len(df)}")
        print(f"✓ CA Total      : {df['montant'].sum()}€")
    else:
        raise FileNotFoundError(f"Fichier introuvable : {chemin}")

task_verifier = PythonOperator(
    task_id         = 'verifier_fichier',
    python_callable = verifier_fichier,
    dag             = dag,
)

# ── TASK 4 : Fin ─────────────────────────────────────────────
task_fin = BashOperator(
    task_id      = 'pipeline_termine',
    bash_command = 'echo "Pipeline du {{ ds }} terminé avec succès ✓"',
    dag          = dag,
)

# ── ORDRE D'EXÉCUTION ────────────────────────────────────────
task_bash >> task_python >> task_verifier >> task_fin
