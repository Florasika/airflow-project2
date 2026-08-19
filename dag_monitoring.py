"""
JOUR 8 / 10 — ETL/Airflow
DAG : Monitoring & Alertes Avancées

Concepts :
    SLA         → Service Level Agreement : délai max d'exécution
    Callbacks   → on_failure, on_retry, on_success, sla_miss
    Métriques   → durée, nb lignes, CA, taux d'erreur
    Health Check→ vérifier l'état du pipeline au fil de l'eau
    Alertes     → notifier l'équipe en cas d'anomalie
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
import pandas as pd
import sqlite3
import time
import random
import os

DB_PATH = '/tmp/monitoring_j8.db'


# ════════════════════════════════════════════════════════════════
#  CALLBACKS — fonctions appelées automatiquement par Airflow
# ════════════════════════════════════════════════════════════════

def on_failure_callback(context):
    """Appelé quand une task échoue."""
    ti      = context['task_instance']
    dag_id  = context['dag'].dag_id
    task_id = ti.task_id
    ds      = context['ds']
    exception = context.get('exception', 'Inconnue')

    msg = (
        f"\n{'='*50}\n"
        f"[ÉCHEC] DAG    : {dag_id}\n"
        f"        Task   : {task_id}\n"
        f"        Date   : {ds}\n"
        f"        Essai  : {ti.try_number}/{ti.max_tries + 1}\n"
        f"        Erreur : {exception}\n"
        f"{'='*50}"
    )
    print(msg)
    # En production :
    # send_slack_alert(msg)
    # send_email_alert(to='equipe@company.com', subject=f'[ALERTE] {dag_id}', body=msg)
    _sauvegarder_metrique('ECHEC', task_id, str(exception), ds)


def on_retry_callback(context):
    """Appelé à chaque retry d'une task."""
    ti      = context['task_instance']
    task_id = ti.task_id
    ds      = context['ds']
    print(f"[RETRY] Task {task_id} — Tentative {ti.try_number} — {ds}")
    _sauvegarder_metrique('RETRY', task_id, f'Tentative {ti.try_number}', ds)


def on_success_callback(context):
    """Appelé quand le DAG entier réussit."""
    dag_id   = context['dag'].dag_id
    ds       = context['ds']
    duration = (datetime.now() -
                context['dag_run'].start_date.replace(tzinfo=None)).seconds
    print(f"[SUCCÈS] {dag_id} terminé en {duration}s pour {ds}")
    _sauvegarder_metrique('SUCCES', 'dag_entier', f'Durée: {duration}s', ds)


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Appelé quand une task dépasse son SLA (délai maximum)."""
    for sla in slas:
        msg = (f"[SLA DÉPASSÉ] DAG: {dag.dag_id} | "
               f"Task: {sla.task_id} | "
               f"SLA: {sla.sla} | "
               f"Date: {sla.execution_date}")
        print(msg)


def _sauvegarder_metrique(type_event, task_id, detail, ds):
    """Sauvegarde un événement dans la table de monitoring."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("""CREATE TABLE IF NOT EXISTS monitoring_events
                        (id INTEGER PRIMARY KEY AUTOINCREMENT,
                         date TEXT, horodatage TEXT, type_event TEXT,
                         task_id TEXT, detail TEXT)""")
        conn.execute("INSERT INTO monitoring_events VALUES (NULL,?,?,?,?,?)",
                     (ds, datetime.now().isoformat(), type_event, task_id, detail))
        conn.commit()
        conn.close()
    except Exception:
        pass  # Ne pas faire échouer le callback si la DB est inaccessible


# ════════════════════════════════════════════════════════════════
#  LE DAG
# ════════════════════════════════════════════════════════════════

@dag(
    dag_id      = 'jour8_monitoring',
    start_date  = datetime(2024, 1, 1),
    schedule    = '@daily',
    catchup     = False,
    tags        = ['jour8', 'monitoring', 'alertes'],
    # SLA global : le DAG doit finir en moins de 30 minutes
    dagrun_timeout = timedelta(minutes=30),
    # Callbacks du DAG entier
    on_success_callback = on_success_callback,
    default_args = {
        'owner'              : 'sung',
        'retries'            : 2,
        'retry_delay'        : timedelta(minutes=3),
        'on_failure_callback': on_failure_callback,
        'on_retry_callback'  : on_retry_callback,
        # SLA par task : doit finir en moins de 10 minutes
        'sla'                : timedelta(minutes=10),
    },
    sla_miss_callback = sla_miss_callback,
)
def pipeline_monitoring():
    """Pipeline avec monitoring complet et alertes."""

    # ── TASK 1 : Initialiser la base de monitoring ───────────
    @task
    def init_monitoring(ds=None):
        conn = sqlite3.connect(DB_PATH)
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS ventes (
                date TEXT, produit TEXT, vendeur TEXT,
                montant REAL, charge_le TEXT
            );
            CREATE TABLE IF NOT EXISTS metriques_pipeline (
                date TEXT PRIMARY KEY,
                nb_lignes INTEGER, ca_total REAL,
                duree_extract_s REAL, duree_transform_s REAL,
                duree_load_s REAL, duree_totale_s REAL,
                taux_erreur REAL, statut TEXT, calcule_le TEXT
            );
            CREATE TABLE IF NOT EXISTS monitoring_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT, horodatage TEXT, type_event TEXT,
                task_id TEXT, detail TEXT
            );
        """)
        conn.close()
        print(f"Init monitoring OK — DB: {DB_PATH}")

    # ── TASK 2 : Extract avec mesure de durée ────────────────
    @task(
        # SLA spécifique à cette task : 5 minutes max
        sla = timedelta(minutes=5),
    )
    def extraire_avec_monitoring(ds=None):
        """Extrait les données en mesurant la durée."""
        debut = time.time()

        random.seed(int(ds.replace('-', '')))
        produits = ['Laptop Pro','Smartphone X','Tablette Air',
                    'Écouteurs BT','Montre Smart']
        prix     = {'Laptop Pro':1200,'Smartphone X':650,'Tablette Air':450,
                    'Écouteurs BT':120,'Montre Smart':280}
        vendeurs = ['Alice','Karim','Lucie','Thomas','Nadia']

        # Simuler un délai d'extraction variable
        time.sleep(random.uniform(0.5, 2.0))

        # Simuler occasionnellement une erreur réseau
        if random.random() < 0.05:
            raise ConnectionError("Timeout lors de l'extraction — retry automatique")

        rows = []
        for _ in range(random.randint(20, 40)):
            p   = random.choice(produits)
            qte = random.randint(1, 10)
            rows.append({
                'date'      : ds,
                'produit'   : p,
                'vendeur'   : random.choice(vendeurs),
                'montant'   : qte * prix[p],
                'charge_le' : datetime.now().isoformat(),
            })

        df   = pd.DataFrame(rows)
        path = f'/tmp/raw_j8_{ds}.csv'
        df.to_csv(path, index=False)

        duree = round(time.time() - debut, 2)
        print(f"Extract OK — {len(df)} lignes — {duree}s")

        return {
            'path'            : path,
            'nb_lignes'       : len(df),
            'ca_brut'         : float(df['montant'].sum()),
            'duree_extract_s' : duree,
        }

    # ── TASK 3 : Transform avec health check ─────────────────
    @task
    def transformer_avec_checks(extract_result: dict, ds=None):
        """Transform avec vérifications de qualité."""
        debut = time.time()
        path  = extract_result['path']
        df    = pd.read_csv(path)

        # Health checks avant transformation
        checks = {
            'pas_de_nulls'      : df.isnull().sum().sum() == 0,
            'montants_positifs' : (df['montant'] > 0).all(),
            'volume_ok'         : len(df) >= 10,
            'vendeurs_valides'  : df['vendeur'].isin(
                ['Alice','Karim','Lucie','Thomas','Nadia']).all(),
        }

        checks_fails = [k for k, v in checks.items() if not v]
        if checks_fails:
            raise ValueError(f"Health checks échoués : {checks_fails}")

        print(f"✓ Health checks OK : {list(checks.keys())}")

        # Transformation
        time.sleep(random.uniform(0.3, 1.0))
        df['marge']   = (df['montant'] * 0.42).round(2)
        df['segment'] = pd.cut(df['montant'],
                               bins=[0,500,2000,float('inf')],
                               labels=['Small','Medium','Large']).astype(str)

        clean_path = f'/tmp/clean_j8_{ds}.csv'
        df.to_csv(clean_path, index=False)

        duree = round(time.time() - debut, 2)
        print(f"Transform OK — {duree}s")

        return {
            **extract_result,
            'clean_path'         : clean_path,
            'duree_transform_s'  : duree,
            'checks'             : checks,
        }

    # ── TASK 4 : Load avec métriques ─────────────────────────
    @task
    def charger_avec_metriques(transform_result: dict, ds=None):
        """Load + calcul des métriques du pipeline."""
        debut = time.time()
        df    = pd.read_csv(transform_result['clean_path'])

        conn = sqlite3.connect(DB_PATH)
        conn.execute(f"DELETE FROM ventes WHERE date='{ds}'")
        df.to_sql('ventes', conn, if_exists='append', index=False)
        conn.commit()

        duree_load  = round(time.time() - debut, 2)
        duree_totale= (transform_result['duree_extract_s'] +
                       transform_result['duree_transform_s'] +
                       duree_load)

        metriques = {
            'date'               : ds,
            'nb_lignes'          : len(df),
            'ca_total'           : round(df['montant'].sum(), 2),
            'duree_extract_s'    : transform_result['duree_extract_s'],
            'duree_transform_s'  : transform_result['duree_transform_s'],
            'duree_load_s'       : duree_load,
            'duree_totale_s'     : round(duree_totale, 2),
            'taux_erreur'        : 0.0,
            'statut'             : 'SUCCES',
            'calcule_le'         : datetime.now().isoformat(),
        }

        conn.execute("DELETE FROM metriques_pipeline WHERE date=?", (ds,))
        conn.execute("""
            INSERT INTO metriques_pipeline VALUES
            (:date,:nb_lignes,:ca_total,:duree_extract_s,
             :duree_transform_s,:duree_load_s,:duree_totale_s,
             :taux_erreur,:statut,:calcule_le)
        """, metriques)
        conn.commit()

        print(f"\n=== Métriques Pipeline {ds} ===")
        for k, v in metriques.items():
            if k not in ['date','calcule_le']:
                print(f"  {k:22} : {v}")

        conn.close()
        return metriques

    # ── TASK 5 : Générer le rapport de monitoring ─────────────
    @task
    def rapport_monitoring(metriques: dict, ds=None):
        """Génère un rapport de monitoring et détecte les anomalies."""
        conn = sqlite3.connect(DB_PATH)

        # Historique des 7 derniers jours
        hist = pd.read_sql("""
            SELECT date, ca_total, nb_lignes, duree_totale_s, statut
            FROM metriques_pipeline
            ORDER BY date DESC
            LIMIT 7
        """, conn)
        conn.close()

        print(f"\n=== Rapport Monitoring (7 derniers jours) ===")
        if len(hist) > 0:
            print(hist.to_string(index=False))

            # Détecter une anomalie de performance
            if len(hist) >= 3:
                duree_moy = hist['duree_totale_s'].mean()
                duree_auj = metriques['duree_totale_s']
                if duree_auj > duree_moy * 1.5:
                    print(f"\n⚠️ ANOMALIE PERF : {duree_auj:.1f}s vs moy {duree_moy:.1f}s")

            # Détecter une baisse de volume
            if len(hist) >= 2:
                vol_moy = hist['nb_lignes'].mean()
                vol_auj = metriques['nb_lignes']
                if vol_auj < vol_moy * 0.7:
                    print(f"\n⚠️ BAISSE VOLUME : {vol_auj} lignes vs moy {vol_moy:.0f}")

        print(f"\n✓ Monitoring OK pour {ds}")

    # ── Enchaînement ─────────────────────────────────────────
    init_monitoring()
    extract  = extraire_avec_monitoring()
    transform= transformer_avec_checks(extract)
    metriques= charger_avec_metriques(transform)
    rapport_monitoring(metriques)

dag_instance = pipeline_monitoring()
