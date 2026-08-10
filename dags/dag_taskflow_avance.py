"""
JOUR 4 / 10 — ETL/Airflow
DAG : TaskFlow API Avancée

Concepts avancés :
    - @task avec multiple_outputs=True
    - @task_group pour regrouper des tasks
    - Mélanger TaskFlow et Operators classiques
    - @task avec trigger_rule
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import pandas as pd
import random
import sqlite3

@dag(
    dag_id      = 'jour4_taskflow_avance',
    start_date  = datetime(2024, 1, 1),
    schedule    = '@daily',
    catchup     = False,
    tags        = ['jour4', 'taskflow', 'avance'],
    default_args= {'owner': 'sung', 'retries': 1,
                   'retry_delay': timedelta(minutes=2)},
)
def pipeline_avance():
    """Pipeline TaskFlow avancé avec task groups et multiple outputs."""

    # ── Task de départ classique (Operator standard) ──────────
    debut = EmptyOperator(task_id='debut_pipeline')

    # ── @task_group : regrouper des tasks dans l'UI ───────────
    @task_group(group_id='extraction_regionale')
    def extraire_par_region(ds=None):
        """Groupe de tasks : 3 extractions en parallèle."""

        @task(task_id='extraire_idf')
        def extraire_idf(ds=None):
            random.seed(1)
            data = [{'region':'IDF','vendeur':v,'ca':random.randint(5000,50000)}
                    for v in ['Alice','Karim']]
            path = f'/tmp/idf_{ds}.csv'
            pd.DataFrame(data).to_csv(path, index=False)
            print(f"IDF extrait : {len(data)} vendeurs")
            return path

        @task(task_id='extraire_paca')
        def extraire_paca(ds=None):
            random.seed(2)
            data = [{'region':'PACA','vendeur':v,'ca':random.randint(3000,40000)}
                    for v in ['Lucie','Thomas']]
            path = f'/tmp/paca_{ds}.csv'
            pd.DataFrame(data).to_csv(path, index=False)
            print(f"PACA extrait : {len(data)} vendeurs")
            return path

        @task(task_id='extraire_ge')
        def extraire_ge(ds=None):
            random.seed(3)
            data = [{'region':'GE','vendeur':'Nadia',
                     'ca':random.randint(2000,30000)}]
            path = f'/tmp/ge_{ds}.csv'
            pd.DataFrame(data).to_csv(path, index=False)
            print(f"Grand Est extrait : {len(data)} vendeurs")
            return path

        return extraire_idf(), extraire_paca(), extraire_ge()

    # ── @task avec multiple_outputs=True ─────────────────────
    # Retourne un dict — chaque clé devient un XCom séparé
    @task(multiple_outputs=True)
    def consolider_et_calculer(ds=None):
        """
        Consolide les 3 fichiers régionaux et calcule des KPIs.
        multiple_outputs=True permet d'accéder à chaque clé du dict
        comme un output XCom distinct.
        """
        regions_data = []
        for region_prefix in ['idf','paca','ge']:
            try:
                path = f'/tmp/{region_prefix}_{ds}.csv'
                df_r = pd.read_csv(path)
                regions_data.append(df_r)
            except FileNotFoundError:
                print(f"Fichier {region_prefix} non trouvé — ignoré")

        if not regions_data:
            return {'ca_total': 0, 'nb_vendeurs': 0, 'statut': 'vide'}

        df_all = pd.concat(regions_data, ignore_index=True)

        ca_total    = int(df_all['ca'].sum())
        nb_vendeurs = len(df_all)
        statut      = 'excellent' if ca_total > 80000 else \
                      'bon'       if ca_total > 50000 else 'faible'

        print(f"CA Consolidé : {ca_total}€ | Vendeurs : {nb_vendeurs} | Statut : {statut}")

        return {
            'ca_total'   : ca_total,
            'nb_vendeurs': nb_vendeurs,
            'statut'     : statut,
        }

    # ── Tasks qui utilisent les sorties multiples ─────────────
    @task
    def notifier_si_excellent(statut: str, ca_total: int):
        """S'exécute toujours — la logique est dans le code."""
        if statut == 'excellent':
            print(f"🚀 EXCELLENT ! CA = {ca_total}€ — Notification envoyée à la direction")
        elif statut == 'bon':
            print(f"✓ Bon résultat. CA = {ca_total}€")
        else:
            print(f"⚠️ CA faible : {ca_total}€ — Alerte équipe commerciale")

    @task
    def sauvegarder_kpis(ca_total: int, nb_vendeurs: int, ds=None):
        """Sauvegarde les KPIs dans SQLite."""
        conn = sqlite3.connect('/tmp/entrepot/kpis.db')
        conn.execute("""CREATE TABLE IF NOT EXISTS kpis
                        (date TEXT, ca_total INTEGER, nb_vendeurs INTEGER)""")
        conn.execute(f"DELETE FROM kpis WHERE date='{ds}'")
        conn.execute(f"INSERT INTO kpis VALUES ('{ds}', {ca_total}, {nb_vendeurs})")
        conn.commit()
        conn.close()
        print(f"KPIs sauvegardés : CA={ca_total}€, Vendeurs={nb_vendeurs}")

    # ── Task finale classique (Operator standard) ─────────────
    fin = BashOperator(
        task_id      = 'fin_pipeline',
        bash_command = 'echo "Pipeline Jour 4 terminé — {{ ds }} ✓"',
        trigger_rule = 'none_failed_min_one_success',
    )

    # ── Enchaînement ──────────────────────────────────────────
    # Mélange de TaskFlow (inférence automatique) et Operators (>>)
    chemins   = extraire_par_region()
    kpis      = consolider_et_calculer()

    # Les dépendances sont automatiquement inférées pour les @task
    # Mais on peut aussi forcer avec >> pour les Operators classiques
    debut >> chemins
    notifier_si_excellent(kpis['statut'], kpis['ca_total'])
    sauvegarder_kpis(kpis['ca_total'], kpis['nb_vendeurs']) >> fin

dag_instance = pipeline_avance()
