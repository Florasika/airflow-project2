"""
JOUR 2 / 10 — ETL/Airflow
DAG : Dépendances complexes — parallèle, convergence, trigger_rule

Montre comment :
- Exécuter des tasks EN PARALLÈLE
- Converger plusieurs branches vers une task finale
- Utiliser trigger_rule pour gérer les cas d'échec
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pandas as pd, random

default_args = {
    'owner'      : 'sung',
    'start_date' : datetime(2024, 1, 1),
    'retries'    : 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    dag_id      = 'jour2_dependances',
    default_args= default_args,
    description = 'Parallèle, convergence, trigger_rule',
    schedule    = '@daily',
    catchup     = False,
    tags        = ['jour2', 'dependances'],
)


# ── TASK de départ ────────────────────────────────────────────
task_debut = EmptyOperator(task_id='debut', dag=dag)


# ── 3 TASKS PARALLÈLES ────────────────────────────────────────
def extraire_region(region, **context):
    random.seed(42)
    df = pd.DataFrame({
        'vendeur' : ['Alice','Karim','Lucie'][:random.randint(1,3)],
        'ca'      : [random.randint(5000,50000) for _ in range(random.randint(1,3))],
        'region'  : region,
    })
    path = f'/tmp/{region.lower().replace(" ", "_")}_{context["ds"]}.csv'
    df.to_csv(path, index=False)
    print(f"Extrait {len(df)} lignes pour {region} — CA: {df['ca'].sum()}€")
    return path

task_idf = PythonOperator(
    task_id         = 'extraire_idf',
    python_callable = extraire_region,
    op_kwargs       = {'region': 'Île-de-France'},
    dag             = dag,
)

task_paca = PythonOperator(
    task_id         = 'extraire_paca',
    python_callable = extraire_region,
    op_kwargs       = {'region': 'PACA'},
    dag             = dag,
)

task_ge = PythonOperator(
    task_id         = 'extraire_grand_est',
    python_callable = extraire_region,
    op_kwargs       = {'region': 'Grand Est'},
    dag             = dag,
)


# ── TASK DE CONVERGENCE ───────────────────────────────────────
def consolider(**context):
    """Récupère les 3 fichiers régionaux et les consolide."""
    ti      = context['ti']
    chemins = [
        ti.xcom_pull(task_ids='extraire_idf'),
        ti.xcom_pull(task_ids='extraire_paca'),
        ti.xcom_pull(task_ids='extraire_grand_est'),
    ]

    # Lire et concaténer uniquement les fichiers disponibles
    dfs = []
    for chemin in chemins:
        if chemin:
            try:
                dfs.append(pd.read_csv(chemin))
            except Exception as e:
                print(f"Impossible de lire {chemin} : {e}")

    if not dfs:
        raise ValueError("Aucune donnée à consolider")

    df_final = pd.concat(dfs, ignore_index=True)
    output   = f'/tmp/consolide_{context["ds"]}.csv'
    df_final.to_csv(output, index=False)

    print(f"Consolidé : {len(df_final)} lignes — CA Total: {df_final['ca'].sum()}€")
    return output

# trigger_rule='all_done' : s'exécute même si certaines tasks parents ont échoué
# Autres valeurs utiles :
#   'all_success'              (défaut) : tous les parents doivent réussir
#   'all_done'                          : tous terminés, succès ou échec
#   'one_success'                       : au moins un parent a réussi
#   'none_failed_min_one_success'       : aucun échec + au moins un succès
task_consolider = PythonOperator(
    task_id         = 'consolider',
    python_callable = consolider,
    trigger_rule    = 'all_done',
    dag             = dag,
)


# ── TASK FINALE ───────────────────────────────────────────────
task_fin = EmptyOperator(
    task_id      = 'fin',
    trigger_rule = 'none_failed_min_one_success',
    dag          = dag,
)


# ── ORDRE ────────────────────────────────────────────────────
# debut → 3 tasks en parallèle → consolider → fin
task_debut >> [task_idf, task_paca, task_ge] >> task_consolider >> task_fin
