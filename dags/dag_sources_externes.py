"""
JOUR 7 / 10 — ETL/Airflow
DAG : Intégration Sources Externes

Concepts :
    Multi-sources   → combiner plusieurs sources de données
    API REST        → appels HTTP avec retry et gestion d'erreurs
    Fichiers        → lecture CSV/JSON depuis un dossier d'entrée
    Déduplication   → éviter les doublons entre sources
    Consolidation   → fusionner les données en une vue unifiée
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
import pandas as pd
import sqlite3
import json
import os
import random
import urllib.request
import urllib.error

DB_PATH    = '/tmp/multi_sources.db'
INBOX_PATH = '/tmp/inbox_j7'


@dag(
    dag_id      = 'jour7_sources_externes',
    start_date  = datetime(2024, 1, 1),
    schedule    = '@daily',
    catchup     = False,
    tags        = ['jour7', 'api', 'multi-sources'],
    default_args= {
        'owner'           : 'sung',
        'retries'         : 3,
        'retry_delay'     : timedelta(minutes=2),
        'retry_exponential_backoff': True,  # 2min, 4min, 8min...
    },
)
def pipeline_sources_externes():
    """Pipeline ETL multi-sources : API + fichiers locaux + consolidation."""

    # ── TASK 1 : Initialiser ─────────────────────────────────
    @task
    def initialiser():
        os.makedirs(INBOX_PATH, exist_ok=True)
        conn = sqlite3.connect(DB_PATH)
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS source_api (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT, vendeur TEXT, produit TEXT,
                montant REAL, source TEXT, charge_le TEXT
            );
            CREATE TABLE IF NOT EXISTS source_fichiers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT, region TEXT, ca_region REAL,
                nb_ventes INTEGER, source TEXT, charge_le TEXT
            );
            CREATE TABLE IF NOT EXISTS consolide (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT, dimension TEXT, valeur TEXT,
                ca REAL, source TEXT, charge_le TEXT
            );
        """)
        conn.close()
        print(f"Init OK — inbox: {INBOX_PATH} | db: {DB_PATH}")

    # ── TASK 2 : Appel API avec retry et fallback ────────────
    @task(retries=3, retry_delay=timedelta(seconds=30))
    def appeler_api(ds=None):
        """
        Appelle une API publique (JSONPlaceholder en demo).
        En production : remplacer l'URL par votre API interne.
        Gère : timeout, erreur réseau, réponse invalide.
        """
        url = 'https://url-inexistante-test12345.com/posts'

        try:
            req      = urllib.request.Request(url, headers={'Accept': 'application/json'})
            response = urllib.request.urlopen(req, timeout=10)
            data     = json.loads(response.read().decode())

            # Transformer en données de ventes (simulation)
            random.seed(int(ds.replace('-', '')))
            produits = ['Laptop Pro','Smartphone X','Tablette Air']
            vendeurs = ['Alice','Karim','Lucie','Thomas','Nadia']
            prix     = {'Laptop Pro':1200,'Smartphone X':650,'Tablette Air':450}

            rows = []
            for item in data:
                p   = random.choice(produits)
                qte = random.randint(1, 8)
                rows.append({
                    'date'      : ds,
                    'vendeur'   : vendeurs[item['userId'] % len(vendeurs)],
                    'produit'   : p,
                    'montant'   : qte * prix[p],
                    'source'    : 'API_REST',
                    'charge_le' : datetime.now().isoformat(),
                })

            df   = pd.DataFrame(rows)
            path = f'{INBOX_PATH}/api_{ds}.csv'
            df.to_csv(path, index=False)
            print(f"API ✓ — {len(df)} enregistrements récupérés")
            return {'path': path, 'source': 'API_REST', 'nb': len(df)}

        except urllib.error.URLError as e:
            print(f"Erreur réseau : {e} — utilisation du fallback")
            # Fallback : générer des données localement si l'API est inaccessible
            random.seed(42)
            rows = [{'date':ds,'vendeur':'Alice','produit':'Laptop Pro',
                     'montant':1200,'source':'FALLBACK','charge_le':datetime.now().isoformat()}
                    for _ in range(3)]
            df   = pd.DataFrame(rows)
            path = f'{INBOX_PATH}/api_fallback_{ds}.csv'
            df.to_csv(path, index=False)
            print(f"Fallback activé — {len(df)} lignes générées localement")
            return {'path': path, 'source': 'FALLBACK', 'nb': len(df)}

    # ── TASK 3 : Lire les fichiers du dossier d'entrée ──────
    @task
    def lire_fichiers_locaux(ds=None):
        """
        Lit les fichiers CSV déposés dans le dossier inbox.
        Simule un dépôt de fichiers par un système externe.
        """
        # Créer des fichiers de simulation
        random.seed(int(ds.replace('-', '')) + 1)
        regions = ['Île-de-France','PACA','Grand Est',
                   'Auvergne-Rhône-Alpes','Occitanie']

        fichiers_crees = []
        for region in regions:
            code = region[:3].lower().replace('î','i')
            df   = pd.DataFrame([{
                'date'      : ds,
                'region'    : region,
                'ca_region' : random.randint(10000, 80000),
                'nb_ventes' : random.randint(5, 50),
                'source'    : 'FICHIER_LOCAL',
                'charge_le' : datetime.now().isoformat(),
            }])
            path = f'{INBOX_PATH}/region_{code}_{ds}.csv'
            df.to_csv(path, index=False)
            fichiers_crees.append(path)

        print(f"Fichiers créés : {len(fichiers_crees)}")

        # Lire et consolider tous les fichiers region_*
        dfs = []
        for f in os.listdir(INBOX_PATH):
            if f.startswith('region_') and f.endswith(f'{ds}.csv'):
                dfs.append(pd.read_csv(f'{INBOX_PATH}/{f}'))

        if not dfs:
            print("Aucun fichier région trouvé")
            return None

        df_all = pd.concat(dfs, ignore_index=True)
        path   = f'{INBOX_PATH}/regions_consolide_{ds}.csv'
        df_all.to_csv(path, index=False)
        print(f"Fichiers régions lus : {len(df_all)} lignes")
        return {'path': path, 'source': 'FICHIER_LOCAL', 'nb': len(df_all)}

    # ── TASK 4 : Charger source API en base ──────────────────
    @task
    def charger_api(api_result: dict, ds=None):
        if not api_result or not api_result.get('path'):
            print("Aucune donnée API à charger")
            return 0

        df   = pd.read_csv(api_result['path'])
        conn = sqlite3.connect(DB_PATH)
        conn.execute(f"DELETE FROM source_api WHERE date='{ds}'")
        df.to_sql('source_api', conn, if_exists='append', index=False)
        count = conn.execute(
            f"SELECT COUNT(*) FROM source_api WHERE date='{ds}'"
        ).fetchone()[0]
        conn.commit(); conn.close()
        print(f"Chargé source API : {count} lignes [{api_result['source']}]")
        return count

    # ── TASK 5 : Charger fichiers en base ────────────────────
    @task
    def charger_fichiers(fichiers_result: dict, ds=None):
        if not fichiers_result or not fichiers_result.get('path'):
            print("Aucun fichier à charger")
            return 0

        df   = pd.read_csv(fichiers_result['path'])
        conn = sqlite3.connect(DB_PATH)
        conn.execute(f"DELETE FROM source_fichiers WHERE date='{ds}'")
        df.to_sql('source_fichiers', conn, if_exists='append', index=False)
        count = conn.execute(
            f"SELECT COUNT(*) FROM source_fichiers WHERE date='{ds}'"
        ).fetchone()[0]
        conn.commit(); conn.close()
        print(f"Chargé source fichiers : {count} lignes")
        return count

    # ── TASK 6 : Consolider les deux sources ─────────────────
    @task(trigger_rule='all_done')
    def consolider(nb_api: int, nb_fichiers: int, ds=None):
        """
        Consolide les données des deux sources en une vue unifiée.
        trigger_rule='all_done' : s'exécute même si une source a échoué.
        """
        conn = sqlite3.connect(DB_PATH)

        # Vue consolidée : vendeurs (depuis API) + régions (depuis fichiers)
        vendeurs = pd.read_sql(
            f"SELECT vendeur AS valeur, SUM(montant) AS ca, "
            f"'vendeur' AS dimension, source FROM source_api "
            f"WHERE date='{ds}' GROUP BY vendeur",
            conn
        )

        regions = pd.read_sql(
            f"SELECT region AS valeur, ca_region AS ca, "
            f"'region' AS dimension, source FROM source_fichiers "
            f"WHERE date='{ds}'",
            conn
        )

        dfs = [df for df in [vendeurs, regions] if len(df) > 0]
        if not dfs:
            print("Aucune donnée à consolider")
            conn.close()
            return

        df_final              = pd.concat(dfs, ignore_index=True)
        df_final['date']      = ds
        df_final['charge_le'] = datetime.now().isoformat()

        conn.execute(f"DELETE FROM consolide WHERE date='{ds}'")
        df_final.to_sql('consolide', conn, if_exists='append', index=False)
        conn.commit()

        # Rapport final
        print(f"\n=== Rapport Consolidé {ds} ===")
        print(f"Source API      : {nb_api} lignes")
        print(f"Source Fichiers : {nb_fichiers} lignes")
        print(f"Total consolidé : {len(df_final)} lignes")

        ca_total = pd.read_sql(
            f"SELECT SUM(ca) FROM consolide WHERE date='{ds}' "
            f"AND dimension='vendeur'", conn
        ).iloc[0,0]
        print(f"CA Total (API)  : {ca_total:.0f}€" if ca_total else "CA: N/A")
        conn.close()

    # ── Enchaînement ─────────────────────────────────────────
    initialiser()
    api_result      = appeler_api()
    fichiers_result = lire_fichiers_locaux()
    nb_api          = charger_api(api_result)
    nb_fichiers     = charger_fichiers(fichiers_result)
    consolider(nb_api, nb_fichiers)

dag_instance = pipeline_sources_externes()
