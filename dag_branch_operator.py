"""
JOUR 2 / 10 — ETL/Airflow
DAG : BranchOperator — Logique conditionnelle dans un pipeline

Le BranchOperator choisit dynamiquement quelle task exécuter
selon une condition calculée à l'exécution.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import pandas as pd
import random

default_args = {
    'owner'           : 'sung',
    'depends_on_past' : False,
    'start_date'      : datetime(2024, 1, 1),
    'retries'         : 1,
    'retry_delay'     : timedelta(minutes=5),
}

dag = DAG(
    dag_id      = 'jour2_branch_operator',
    default_args= default_args,
    description = 'BranchOperator — pipelines conditionnels',
    schedule    = '@daily',
    catchup     = False,
    tags        = ['jour2', 'branch'],
)


# ── TASK 1 : Générer des données ─────────────────────────────
def generer_ventes(**context):
    random.seed(int(context['ds'].replace('-', '')))
    produits = ['Laptop Pro','Smartphone X','Tablette Air','Écouteurs BT']
    prix     = {'Laptop Pro':1200,'Smartphone X':650,'Tablette Air':450,'Écouteurs BT':120}

    rows = []
    for _ in range(random.randint(5, 25)):
        p   = random.choice(produits)
        qte = random.randint(1, 10)
        rows.append({'produit':p, 'quantite':qte, 'montant':qte*prix[p]})

    df       = pd.DataFrame(rows)
    ca_total = df['montant'].sum()
    path     = f'/tmp/ventes_{context["ds"]}.csv'
    df.to_csv(path, index=False)

    # Pousser le CA dans XCom pour que le BranchOperator y accède
    context['ti'].xcom_push(key='ca_total', value=int(ca_total))
    print(f"CA Total du jour : {ca_total}€ — {len(df)} lignes")
    return path

task_generer = PythonOperator(
    task_id='generer_ventes',
    python_callable=generer_ventes,
    dag=dag,
)


# ── TASK 2 : BranchOperator — décide quelle branche suivre ──
def choisir_traitement(**context):
    """
    Retourne le task_id de la prochaine task à exécuter.
    BranchPythonOperator attend un task_id (ou une liste) en retour.
    """
    ti       = context['ti']
    ca_total = ti.xcom_pull(task_ids='generer_ventes', key='ca_total')

    print(f"CA Total reçu : {ca_total}€")

    if ca_total >= 10000:
        print("→ Branche : traitement_gros_volume")
        return 'traitement_gros_volume'
    elif ca_total >= 3000:
        print("→ Branche : traitement_volume_moyen")
        return 'traitement_volume_moyen'
    else:
        print("→ Branche : traitement_faible_volume")
        return 'traitement_faible_volume'

task_branch = BranchPythonOperator(
    task_id='choisir_traitement',
    python_callable=choisir_traitement,
    dag=dag,
)


# ── TASKS des branches ────────────────────────────────────────
task_gros = BashOperator(
    task_id      = 'traitement_gros_volume',
    bash_command = 'echo "🚀 Gros volume détecté — envoi rapport direction"',
    dag          = dag,
)

task_moyen = BashOperator(
    task_id      = 'traitement_volume_moyen',
    bash_command = 'echo "✓ Volume normal — traitement standard"',
    dag          = dag,
)

task_faible = BashOperator(
    task_id      = 'traitement_faible_volume',
    bash_command = 'echo "⚠️ Faible volume — alerte équipe commerciale"',
    dag          = dag,
)


# ── TASK finale : converge après les 3 branches ──────────────
# trigger_rule='none_failed_min_one_success' = s'exécute dès qu'au
# moins une branche précédente a réussi (et aucune n'a échoué)
task_fin = EmptyOperator(
    task_id      = 'fin_pipeline',
    trigger_rule = 'none_failed_min_one_success',
    dag          = dag,
)


# ── ORDRE ────────────────────────────────────────────────────
task_generer >> task_branch
task_branch  >> [task_gros, task_moyen, task_faible]
[task_gros, task_moyen, task_faible] >> task_fin
