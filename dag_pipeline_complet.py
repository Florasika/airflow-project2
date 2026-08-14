"""
JOUR 5 / 10 — ETL/Airflow
DAG : Pipeline ETL Complet avec SQLite

Concepts :
    Idempotence  → le pipeline peut tourner plusieurs fois sans dupliquer
    Monitoring   → callbacks on_success / on_failure
    Alertes      → notification en cas d'échec
    Data Quality → vérifications avant chargement
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
import pandas as pd
import sqlite3
import os
import random

DB_PATH = '/tmp/entrepot_complet.db'


# ── Callbacks : appelés automatiquement par Airflow ──────────
def on_failure_callback(context):
    """Appelé quand une task échoue."""
    dag_id  = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    ds      = context['ds']
    print(f"[ALERTE] Échec du DAG {dag_id} — Task {task_id} — Date {ds}")
    # En production : envoyer un email, une notification Slack, etc.

def on_success_callback(context):
    """Appelé quand le DAG entier réussit."""
    dag_id = context['dag'].dag_id
    ds     = context['ds']
    print(f"[OK] DAG {dag_id} terminé avec succès pour le {ds}")


@dag(
    dag_id      = 'jour5_pipeline_complet',
    start_date  = datetime(2024, 1, 1),
    schedule    = '@daily',
    catchup     = False,
    tags        = ['jour5', 'pipeline', 'complet'],
    default_args= {
        'owner'              : 'sung',
        'retries'            : 2,
        'retry_delay'        : timedelta(minutes=5),
        'on_failure_callback': on_failure_callback,
    },
    on_success_callback = on_success_callback,
)
def pipeline_etl_complet():
    """
    Pipeline ETL complet et idempotent :
    Init → Extract → Validate → Transform → Load → Verify → Report
    """

    # ── TASK 1 : Initialiser la base ─────────────────────────
    @task
    def init_database():
        """Crée les tables si elles n'existent pas (idempotent)."""
        conn = sqlite3.connect(DB_PATH)
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS ventes_raw (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                date      TEXT NOT NULL,
                produit   TEXT NOT NULL,
                categorie TEXT NOT NULL,
                vendeur   TEXT NOT NULL,
                region    TEXT NOT NULL,
                quantite  INTEGER NOT NULL,
                montant   REAL NOT NULL,
                charge_le TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ventes_clean (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                date          TEXT NOT NULL,
                produit       TEXT NOT NULL,
                categorie     TEXT NOT NULL,
                vendeur       TEXT NOT NULL,
                region        TEXT NOT NULL,
                quantite      INTEGER NOT NULL,
                montant       REAL NOT NULL,
                marge         REAL NOT NULL,
                taille_vente  TEXT NOT NULL,
                charge_le     TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS kpis_journaliers (
                date         TEXT PRIMARY KEY,
                ca_total     REAL,
                marge_totale REAL,
                taux_marge   REAL,
                nb_ventes    INTEGER,
                top_produit  TEXT,
                top_vendeur  TEXT,
                calcule_le   TEXT
            );
        """)
        conn.close()
        print(f"Base initialisée : {DB_PATH}")
        return DB_PATH

    # ── TASK 2 : Extraire les données ────────────────────────
    @task
    def extraire(db_path: str, ds=None):
        """Extrait les données du jour depuis la source."""
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

        rows = []
        for _ in range(random.randint(20, 35)):
            p   = random.choice(produits)
            qte = random.randint(1, 12)
            rows.append({
                'date'     : ds,
                'produit'  : p,
                'categorie': categories[p],
                'vendeur'  : random.choice(vendeurs),
                'region'   : random.choice(regions),
                'quantite' : qte,
                'montant'  : qte * prix[p],
                'charge_le': datetime.now().isoformat(),
            })

        df   = pd.DataFrame(rows)
        path = f'/tmp/raw_j5_{ds}.csv'
        df.to_csv(path, index=False)

        # Charger dans la table raw (idempotent : supprimer d'abord)
        conn = sqlite3.connect(db_path)
        conn.execute(f"DELETE FROM ventes_raw WHERE date = '{ds}'")
        df.to_sql('ventes_raw', conn, if_exists='append', index=False)
        conn.commit()
        conn.close()

        print(f"Extrait {len(df)} lignes — CA: {df['montant'].sum():.0f}€")
        return path

    # ── TASK 3 : Valider la qualité des données ──────────────
    @task(multiple_outputs=True)
    def valider(path: str, ds=None):
        """
        Vérifie la qualité des données avant transformation.
        Retourne un rapport de validation.
        """
        df = pd.read_csv(path)
        erreurs = []

        # Check 1 : pas de valeurs nulles
        nulls = df.isnull().sum().sum()
        if nulls > 0:
            erreurs.append(f"{nulls} valeurs nulles détectées")

        # Check 2 : montants positifs
        montants_negatifs = (df['montant'] <= 0).sum()
        if montants_negatifs > 0:
            erreurs.append(f"{montants_negatifs} montants négatifs ou nuls")

        # Check 3 : volume minimum
        if len(df) < 5:
            erreurs.append(f"Volume trop faible : {len(df)} lignes (minimum 5)")

        # Check 4 : vendeurs valides
        vendeurs_valides = {'Alice','Karim','Lucie','Thomas','Nadia'}
        vendeurs_inconnus = set(df['vendeur']) - vendeurs_valides
        if vendeurs_inconnus:
            erreurs.append(f"Vendeurs inconnus : {vendeurs_inconnus}")

        if erreurs:
            raise ValueError(f"Validation échouée : {' | '.join(erreurs)}")

        print(f"✓ Validation OK — {len(df)} lignes, "
              f"CA: {df['montant'].sum():.0f}€, "
              f"0 erreur")

        return {
            'nb_lignes' : len(df),
            'ca_brut'   : float(df['montant'].sum()),
            'is_valid'  : True,
        }

    # ── TASK 4 : Transformer ─────────────────────────────────
    @task
    def transformer(path: str, db_path: str, ds=None):
        """Enrichit et nettoie les données."""
        df = pd.read_csv(path)

        # Enrichissements
        df['marge']       = (df['montant'] * 0.42).round(2)
        df['taille_vente']= pd.cut(
            df['montant'],
            bins=[0, 500, 2000, float('inf')],
            labels=['Petite','Moyenne','Grosse']
        ).astype(str)
        df['charge_le']   = datetime.now().isoformat()

        # Charger dans la table clean (idempotent)
        conn = sqlite3.connect(db_path)
        conn.execute(f"DELETE FROM ventes_clean WHERE date = '{ds}'")
        df.to_sql('ventes_clean', conn, if_exists='append', index=False)
        conn.commit()
        conn.close()

        print(f"Transformé : {len(df)} lignes — "
              f"Marge totale: {df['marge'].sum():.0f}€")
        return len(df)

    # ── TASK 5 : Calculer les KPIs journaliers ───────────────
    @task
    def calculer_kpis(nb_lignes: int, db_path: str, ds=None):
        """Agrège les KPIs du jour et les stocke."""
        conn = sqlite3.connect(db_path)

        kpis = conn.execute(f"""
            SELECT
                '{ds}'                                   AS date,
                ROUND(SUM(montant), 2)                   AS ca_total,
                ROUND(SUM(marge), 2)                     AS marge_totale,
                ROUND(SUM(marge)*100.0/SUM(montant), 1)  AS taux_marge,
                COUNT(*)                                  AS nb_ventes,
                (SELECT produit FROM ventes_clean
                 WHERE date='{ds}'
                 GROUP BY produit ORDER BY SUM(montant) DESC LIMIT 1) AS top_produit,
                (SELECT vendeur FROM ventes_clean
                 WHERE date='{ds}'
                 GROUP BY vendeur ORDER BY SUM(montant) DESC LIMIT 1) AS top_vendeur,
                '{datetime.now().isoformat()}'            AS calcule_le
            FROM ventes_clean
            WHERE date = '{ds}'
        """).fetchone()

        conn.execute(f"DELETE FROM kpis_journaliers WHERE date = '{ds}'")
        conn.execute("""
            INSERT INTO kpis_journaliers
            VALUES (?,?,?,?,?,?,?,?)
        """, kpis)
        conn.commit()

        print(f"KPIs calculés pour {ds} :")
        cols = ['date','ca_total','marge_totale','taux_marge',
                'nb_ventes','top_produit','top_vendeur','calcule_le']
        for col, val in zip(cols, kpis):
            print(f"  {col:15} : {val}")
        conn.close()
        return dict(zip(cols, kpis))

    # ── TASK 6 : Vérification finale ─────────────────────────
    @task
    def verifier_chargement(kpis: dict, ds=None):
        """Vérifie que les données ont bien été chargées."""
        conn = sqlite3.connect(DB_PATH)

        counts = {
            'raw'  : conn.execute(f"SELECT COUNT(*) FROM ventes_raw   WHERE date='{ds}'").fetchone()[0],
            'clean': conn.execute(f"SELECT COUNT(*) FROM ventes_clean  WHERE date='{ds}'").fetchone()[0],
            'kpis' : conn.execute(f"SELECT COUNT(*) FROM kpis_journaliers WHERE date='{ds}'").fetchone()[0],
        }
        conn.close()

        print(f"Vérification pour {ds} :")
        for table, count in counts.items():
            status = '✓' if count > 0 else '✗'
            print(f"  {status} {table:10} : {count} lignes")

        if any(c == 0 for c in counts.values()):
            raise ValueError(f"Chargement incomplet : {counts}")

        print(f"\nCA du jour : {kpis.get('ca_total')}€")
        print(f"Top Produit : {kpis.get('top_produit')}")
        print(f"Top Vendeur : {kpis.get('top_vendeur')}")

    # ── TASK 7 : Générer le rapport ───────────────────────────
    @task
    def generer_rapport(ds=None):
        """Génère un rapport CSV du mois en cours."""
        conn = sqlite3.connect(DB_PATH)

        df = pd.read_sql("""
            SELECT date, ca_total, marge_totale, taux_marge,
                   nb_ventes, top_produit, top_vendeur
            FROM kpis_journaliers
            ORDER BY date DESC
        """, conn)
        conn.close()

        if len(df) == 0:
            print("Aucune donnée pour le rapport")
            return

        rapport_path = f'/tmp/rapport_mensuel_{ds[:7]}.csv'
        df.to_csv(rapport_path, index=False)

        print(f"Rapport généré : {rapport_path}")
        print(f"Jours couverts : {len(df)}")
        print(f"CA Cumulé     : {df['ca_total'].sum():.0f}€")
        print(f"Marge Moy.    : {df['taux_marge'].mean():.1f}%")

    # ── ENCHAÎNEMENT ─────────────────────────────────────────
    db_path     = init_database()
    raw_path    = extraire(db_path)
    validation  = valider(raw_path)
    nb_lignes   = transformer(raw_path, db_path)
    kpis        = calculer_kpis(nb_lignes, db_path)
    verifier_chargement(kpis)
    generer_rapport()

dag_instance = pipeline_etl_complet()
