"""
JOUR 10 / 10 — ETL/Airflow
DAG : Projet Final — Pipeline ETL Complet

Combine tout ce qu'on a appris en 10 jours :
    J1  → BashOperator, PythonOperator, pattern ETL
    J2  → BranchOperator, parallélisme, trigger_rule
    J3  → Hooks, connexions SQLite
    J4  → TaskFlow API (@task, @dag, @task_group)
    J5  → Idempotence, Data Quality, callbacks
    J6  → Variables Airflow, Jinja, Dynamic DAGs
    J7  → Multi-sources, retry, fallback
    J8  → SLA, monitoring, métriques
    J9  → Fonctions pures testables
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import pandas as pd
import sqlite3
import json
import time
import random
import os

DB_PATH   = '/tmp/final_j10.db'
INBOX     = '/tmp/inbox_j10'

# ════════════════════════════════════════════════════════════════
#  FONCTIONS PURES — testables avec pytest (J9)
# ════════════════════════════════════════════════════════════════

def valider_dataframe(df: pd.DataFrame, min_lignes: int = 5) -> dict:
    erreurs = []
    if len(df) < min_lignes:
        erreurs.append(f"Volume insuffisant : {len(df)} < {min_lignes}")
    if df.isnull().sum().sum() > 0:
        erreurs.append(f"Valeurs nulles : {df.isnull().sum().sum()}")
    if 'montant' in df.columns and (df['montant'] <= 0).any():
        erreurs.append("Montants négatifs ou nuls")
    return {'valid': len(erreurs) == 0, 'erreurs': erreurs, 'nb': len(df)}


def enrichir_donnees(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['marge']        = (df['montant'] * 0.42).round(2)
    df['taux_marge']   = 0.42
    df['taille_vente'] = pd.cut(
        df['montant'],
        bins=[0, 500, 2000, float('inf')],
        labels=['Petite','Moyenne','Grosse']
    ).astype(str)
    df['charge_le']    = datetime.now().isoformat()
    return df


def calculer_kpis(df: pd.DataFrame, objectif: float = 50000) -> dict:
    if len(df) == 0:
        return {'ca_total':0,'marge_totale':0,'taux_marge':0,
                'nb_ventes':0,'atteinte_obj':0,'statut':'vide'}
    ca     = round(float(df['montant'].sum()), 2)
    marge  = round(float(df['marge'].sum()), 2)
    return {
        'ca_total'     : ca,
        'marge_totale' : marge,
        'taux_marge'   : round(marge/ca*100, 1) if ca > 0 else 0,
        'nb_ventes'    : len(df),
        'panier_moyen' : round(ca/len(df), 2),
        'top_produit'  : df.groupby('produit')['montant'].sum().idxmax(),
        'top_vendeur'  : df.groupby('vendeur')['montant'].sum().idxmax(),
        'atteinte_obj' : round(ca/objectif*100, 1),
        'statut'       : 'excellent' if ca >= objectif else
                         'bon'       if ca >= objectif*0.8 else 'faible',
    }


# ════════════════════════════════════════════════════════════════
#  CALLBACKS — monitoring (J8)
# ════════════════════════════════════════════════════════════════

def on_failure_callback(context):
    ti   = context['task_instance']
    print(f"[ÉCHEC] {context['dag'].dag_id}/{ti.task_id} "
          f"— {context['ds']} — Essai {ti.try_number}")
    _log_event('ECHEC', ti.task_id, str(context.get('exception','')), context['ds'])


def on_success_callback(context):
    dur = (datetime.now() -
           context['dag_run'].start_date.replace(tzinfo=None)).seconds
    print(f"[SUCCÈS] Pipeline terminé en {dur}s — {context['ds']}")


def _log_event(type_event, task_id, detail, ds):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("""CREATE TABLE IF NOT EXISTS events
                        (ts TEXT, type TEXT, task TEXT, detail TEXT, date TEXT)""")
        conn.execute("INSERT INTO events VALUES (?,?,?,?,?)",
                     (datetime.now().isoformat(), type_event, task_id, detail, ds))
        conn.commit(); conn.close()
    except Exception:
        pass


# ════════════════════════════════════════════════════════════════
#  LE DAG FINAL
# ════════════════════════════════════════════════════════════════

@dag(
    dag_id      = 'jour10_pipeline_final',
    start_date  = datetime(2024, 1, 1),
    schedule    = '@daily',
    catchup     = False,
    tags        = ['jour10', 'final', 'complet'],
    dagrun_timeout      = timedelta(minutes=30),
    on_success_callback = on_success_callback,
    default_args = {
        'owner'              : 'sung',
        'retries'            : 2,
        'retry_delay'        : timedelta(minutes=3),
        'retry_exponential_backoff': True,
        'on_failure_callback': on_failure_callback,
        'sla'                : timedelta(minutes=15),
    },
    # Params modifiables au déclenchement (J6)
    params = {
        'objectif_ca' : 50000,
        'min_lignes'  : 10,
        'mode'        : 'prod',
    },
)
def pipeline_final():
    """
    Pipeline ETL Final — 10 jours de concepts en un seul DAG.
    """

    # ── TASK 1 : Init + config (J6 — Variables) ──────────────
    @task
    def init_et_config(ds=None, params=None):
        os.makedirs(INBOX, exist_ok=True)

        # Lire les Variables Airflow (J6)
        try:
            env = Variable.get('environnement', default_var='dev')
        except Exception:
            env = 'dev'

        config = {
            'env'         : env,
            'objectif_ca' : params.get('objectif_ca', 50000),
            'min_lignes'  : params.get('min_lignes', 10),
            'mode'        : params.get('mode', 'dev'),
            'db_path'     : DB_PATH,
        }

        # Initialiser la BDD (J5 — idempotent)
        conn = sqlite3.connect(DB_PATH)
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS ventes_raw (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT, source TEXT, produit TEXT, categorie TEXT,
                vendeur TEXT, region TEXT, quantite INTEGER,
                montant REAL, charge_le TEXT
            );
            CREATE TABLE IF NOT EXISTS ventes_clean (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT, produit TEXT, categorie TEXT,
                vendeur TEXT, region TEXT, quantite INTEGER,
                montant REAL, marge REAL, taille_vente TEXT, charge_le TEXT
            );
            CREATE TABLE IF NOT EXISTS kpis_journaliers (
                date TEXT PRIMARY KEY, ca_total REAL, marge_totale REAL,
                taux_marge REAL, nb_ventes INTEGER, panier_moyen REAL,
                top_produit TEXT, top_vendeur TEXT, atteinte_obj REAL,
                statut TEXT, duree_totale_s REAL, calcule_le TEXT
            );
            CREATE TABLE IF NOT EXISTS events (
                ts TEXT, type TEXT, task TEXT, detail TEXT, date TEXT
            );
        """)
        conn.close()

        print(f"Config chargée : env={env} | objectif={config['objectif_ca']}€")
        return config

    # ── TASK GROUP : Extraction multi-sources (J7, J4) ───────
    @task_group(group_id='extraction')
    def groupe_extraction(config: dict, ds=None):

        @task
        def extraire_source_principale(config: dict, ds=None):
            """Source principale — données de ventes (J7 — retry, fallback)."""
            debut = time.time()
            random.seed(int(ds.replace('-', '')))

            produits   = ['Laptop Pro','Smartphone X','Tablette Air',
                          'Écouteurs BT','Montre Smart']
            categories = {'Laptop Pro':'Informatique','Smartphone X':'Mobile',
                          'Tablette Air':'Informatique','Écouteurs BT':'Audio',
                          'Montre Smart':'Wearable'}
            prix       = {'Laptop Pro':1200,'Smartphone X':650,'Tablette Air':450,
                          'Écouteurs BT':120,'Montre Smart':280}
            vendeurs   = ['Alice','Karim','Lucie','Thomas','Nadia']
            regions    = ['Île-de-France','PACA','Grand Est',
                          'Auvergne-Rhône-Alpes','Occitanie']

            # Simuler une erreur réseau occasionnelle (J7 — retry)
            if random.random() < 0.03:
                raise ConnectionError("Timeout source principale — retry")

            rows = []
            for _ in range(random.randint(25, 40)):
                p   = random.choice(produits)
                qte = random.randint(1, 10)
                rows.append({
                    'date'      : ds,
                    'source'    : 'SOURCE_PRINCIPALE',
                    'produit'   : p,
                    'categorie' : categories[p],
                    'vendeur'   : random.choice(vendeurs),
                    'region'    : random.choice(regions),
                    'quantite'  : qte,
                    'montant'   : qte * prix[p],
                    'charge_le' : datetime.now().isoformat(),
                })

            df   = pd.DataFrame(rows)
            path = f'{INBOX}/source_principale_{ds}.csv'
            df.to_csv(path, index=False)

            duree = round(time.time() - debut, 2)
            print(f"Source principale : {len(df)} lignes — {duree}s")
            return {'path': path, 'nb': len(df), 'duree': duree}

        @task
        def extraire_source_secondaire(ds=None):
            """Source secondaire — objectifs par région."""
            random.seed(int(ds.replace('-', '')) + 999)
            regions = ['Île-de-France','PACA','Grand Est',
                       'Auvergne-Rhône-Alpes','Occitanie']

            rows = [{'date':ds,'region':r,
                     'objectif_region':random.randint(8000,20000),
                     'source':'SOURCE_SECONDAIRE'} for r in regions]
            df   = pd.DataFrame(rows)
            path = f'{INBOX}/objectifs_{ds}.csv'
            df.to_csv(path, index=False)
            print(f"Source secondaire : {len(df)} lignes (objectifs régions)")
            return {'path': path, 'nb': len(df)}

        return extraire_source_principale(config), extraire_source_secondaire()

    # ── TASK : Data Quality (J5, J9) ─────────────────────────
    @task
    def valider_qualite(resultats_extraction: tuple, config: dict):
        principale, secondaire = resultats_extraction
        df  = pd.read_csv(principale['path'])

        # Utilise la fonction pure testable (J9)
        rapport = valider_dataframe(df, min_lignes=config.get('min_lignes', 10))

        if not rapport['valid']:
            raise ValueError(f"Qualité insuffisante : {rapport['erreurs']}")

        print(f"✓ Qualité OK : {rapport['nb']} lignes validées")
        return principale['path']

    # ── TASK : BranchOperator selon volume (J2) ───────────────
    @task
    def choisir_strategie(path: str, config: dict) -> str:
        df       = pd.read_csv(path)
        ca_brut  = df['montant'].sum()
        objectif = config.get('objectif_ca', 50000)

        if ca_brut >= objectif:
            strategie = 'traitement_standard'
        else:
            strategie = 'traitement_volume_faible'

        print(f"CA brut: {ca_brut:.0f}€ | Objectif: {objectif}€ "
              f"→ Stratégie: {strategie}")
        return strategie

    # ── TASK : Transform (J5, J9) ─────────────────────────────
    @task
    def transformer(path: str, strategie: str, ds=None):
        debut = time.time()
        df    = pd.read_csv(path)

        # Utilise la fonction pure testable (J9)
        df_clean = enrichir_donnees(df)

        # Stratégie différente selon le volume (J2)
        if strategie == 'traitement_volume_faible':
            print("Mode volume faible — enrichissement simplifié")
        else:
            print("Mode standard — enrichissement complet")

        clean_path = f'/tmp/clean_final_{ds}.csv'
        df_clean.to_csv(clean_path, index=False)

        duree = round(time.time() - debut, 2)
        print(f"Transform OK — {len(df_clean)} lignes — {duree}s")
        return {'path': clean_path, 'nb': len(df_clean), 'duree': duree}

    # ── TASK : Load idempotent (J5) ───────────────────────────
    @task
    def charger(transform_result: dict, ds=None):
        debut = time.time()
        df    = pd.read_csv(transform_result['path'])

        conn  = sqlite3.connect(DB_PATH)
        conn.execute(f"DELETE FROM ventes_clean WHERE date='{ds}'")
        df.to_sql('ventes_clean', conn, if_exists='append', index=False)
        conn.commit()

        count = conn.execute(
            f"SELECT COUNT(*) FROM ventes_clean WHERE date='{ds}'"
        ).fetchone()[0]
        conn.close()

        duree = round(time.time() - debut, 2)
        print(f"Load OK — {count} lignes en {duree}s")
        return {'nb': count, 'duree': duree}

    # ── TASK : KPIs + métriques (J8, J9) ─────────────────────
    @task
    def calculer_et_sauvegarder_kpis(
            load_result: dict, config: dict,
            transform_result: dict, extract_result: tuple, ds=None):

        df    = pd.read_sql(
            f"SELECT * FROM ventes_clean WHERE date='{ds}'",
            sqlite3.connect(DB_PATH)
        )

        # Utilise la fonction pure testable (J9)
        kpis = calculer_kpis(df, objectif=config.get('objectif_ca', 50000))

        # Durée totale du pipeline (J8)
        principale, secondaire = extract_result
        duree_totale = round(
            principale.get('duree', 0) +
            transform_result.get('duree', 0) +
            load_result.get('duree', 0), 2
        )

        conn = sqlite3.connect(DB_PATH)
        conn.execute("DELETE FROM kpis_journaliers WHERE date=?", (ds,))
        conn.execute("""
            INSERT INTO kpis_journaliers VALUES
            (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            ds,
            kpis['ca_total'], kpis['marge_totale'], kpis['taux_marge'],
            kpis['nb_ventes'], kpis['panier_moyen'],
            kpis['top_produit'], kpis['top_vendeur'],
            kpis['atteinte_obj'], kpis['statut'],
            duree_totale, datetime.now().isoformat()
        ))
        conn.commit()

        print(f"\n{'='*45}")
        print(f"  RAPPORT FINAL — {ds}")
        print(f"{'='*45}")
        for k, v in kpis.items():
            print(f"  {k:15} : {v}")
        print(f"  {'duree_totale':15} : {duree_totale}s")
        print(f"{'='*45}")

        conn.close()
        return {**kpis, 'duree_totale_s': duree_totale}

    # ── TASK : Rapport final (J8 — détection anomalies) ──────
    @task(trigger_rule='all_done')
    def rapport_final(kpis: dict, ds=None):
        conn = sqlite3.connect(DB_PATH)

        hist = pd.read_sql("""
            SELECT date, ca_total, nb_ventes, taux_marge,
                   atteinte_obj, statut, duree_totale_s
            FROM kpis_journaliers
            ORDER BY date DESC LIMIT 10
        """, conn)
        conn.close()

        print(f"\n=== Historique 10 derniers jours ===")
        if len(hist) > 0:
            print(hist.to_string(index=False))

            if len(hist) >= 3:
                duree_moy = hist['duree_totale_s'].mean()
                if kpis.get('duree_totale_s',0) > duree_moy * 1.5:
                    print(f"\n⚠️ ANOMALIE PERF détectée")

        print(f"\n✓ Pipeline Jour 10 terminé pour {ds}")
        print(f"  Statut : {kpis.get('statut','?')}")

    # ── ENCHAÎNEMENT FINAL ────────────────────────────────────
    config          = init_et_config()
    extraction      = groupe_extraction(config)
    path_valide     = valider_qualite(extraction, config)
    strategie       = choisir_strategie(path_valide, config)
    transform_res   = transformer(path_valide, strategie)
    load_res        = charger(transform_res)
    kpis            = calculer_et_sauvegarder_kpis(
                          load_res, config, transform_res, extraction)
    rapport_final(kpis)

dag_instance = pipeline_final()
