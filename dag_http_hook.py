"""
JOUR 3 / 10 — ETL/Airflow
DAG : HTTP Hook — appeler une API REST depuis Airflow

Utilise l'API publique JSONPlaceholder (pas de clé nécessaire)
pour démontrer le pattern HttpHook.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
import pandas as pd
import json

default_args = {
    'owner'      : 'sung',
    'start_date' : datetime(2024, 1, 1),
    'retries'    : 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id      = 'jour3_http_hook',
    default_args= default_args,
    description = 'HTTP Hook — appeler une API REST',
    schedule    = '@daily',
    catchup     = False,
    tags        = ['jour3', 'http', 'api'],
)

# ─────────────────────────────────────────────────────────────
# AVANT D'EXÉCUTER CE DAG :
# Configurer la connexion dans Airflow UI :
#   Admin → Connexions → + Ajouter
#   Conn ID   : api_jsonplaceholder
#   Conn Type : HTTP
#   Host      : https://jsonplaceholder.typicode.com
# ─────────────────────────────────────────────────────────────


def appeler_api(**context):
    """
    Appelle l'API JSONPlaceholder via HttpHook.
    En production : remplacer par l'URL de votre API interne.
    """
    # HttpHook utilise la connexion configurée dans l'UI
    hook     = HttpHook(method='GET', http_conn_id='api_jsonplaceholder')
    response = hook.run('/posts?_limit=10')

    data  = json.loads(response.text)
    df    = pd.DataFrame(data)[['id','userId','title']]
    path  = f'/tmp/api_posts_{context["ds"]}.csv'
    df.to_csv(path, index=False)

    print(f"API appelée ✓ — {len(df)} enregistrements récupérés")
    return path

task_api = PythonOperator(
    task_id='appeler_api',
    python_callable=appeler_api,
    dag=dag,
)


def traiter_resultats(**context):
    ti   = context['ti']
    path = ti.xcom_pull(task_ids='appeler_api')
    df   = pd.read_csv(path)

    print(f"Traitement de {len(df)} enregistrements")
    print(df.head().to_string(index=False))

task_traiter = PythonOperator(
    task_id='traiter_resultats',
    python_callable=traiter_resultats,
    dag=dag,
)

task_api >> task_traiter
