"""
JOUR 6 / 10 — ETL/Airflow
DAG : Variables Airflow & Jinja Templating

Concepts :
    Variable   → stocker une config dans Airflow (UI ou CLI)
    Jinja      → templates {{ }} dans les bash_command, sql...
    Params     → passer des valeurs au déclenchement du DAG
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import pandas as pd
import sqlite3
import json

@dag(
    dag_id      = 'jour6_variables_jinja',
    start_date  = datetime(2024, 1, 1),
    schedule    = '@daily',
    catchup     = False,
    tags        = ['jour6', 'variables', 'jinja'],
    default_args= {'owner': 'sung', 'retries': 1,
                   'retry_delay': timedelta(minutes=5)},
    # Params : valeurs modifiables au déclenchement depuis l'UI
    params      = {
        'seuil_ca'   : 5000,
        'nb_lignes_min': 10,
        'envoyer_rapport': True,
    },
)
def pipeline_variables():
    """Pipeline utilisant les Variables Airflow et le templating Jinja."""

    # ── TASK 1 : Lire les Variables Airflow ──────────────────
    @task
    def lire_config(ds=None, params=None):
        """
        Lit la configuration depuis les Variables Airflow.
        Variables configurées dans : Admin → Variables

        Variable.get() avec default_var évite une erreur si la variable n'existe pas.
        Variable.get() avec deserialize_json=True parse le JSON automatiquement.
        """
        # Variables simples
        env          = Variable.get('environnement', default_var='dev')
        db_path      = Variable.get('db_path',       default_var='/tmp/entrepot_j6.db')
        equipe_email = Variable.get('equipe_email',  default_var='equipe@company.com')

        # Variable JSON (stockée comme string JSON dans Airflow)
        try:
            config_prod = Variable.get('config_pipeline', deserialize_json=True)
        except Exception:
            # Valeur par défaut si la variable n'existe pas encore
            config_prod = {
                'seuil_alerte': 3000,
                'regions'     : ['Île-de-France', 'PACA', 'Grand Est'],
                'vendeurs'    : ['Alice', 'Karim', 'Lucie', 'Thomas', 'Nadia'],
            }

        # Params du DAG (modifiables au déclenchement depuis l'UI)
        seuil_ca      = params.get('seuil_ca', 5000)
        nb_lignes_min = params.get('nb_lignes_min', 10)

        config = {
            'env'         : env,
            'db_path'     : db_path,
            'email'       : equipe_email,
            'seuil_ca'    : seuil_ca,
            'nb_lignes_min': nb_lignes_min,
            'regions'     : config_prod.get('regions', []),
            'vendeurs'    : config_prod.get('vendeurs', []),
        }

        print(f"Config chargée :")
        for k, v in config.items():
            print(f"  {k:15} : {v}")

        return config

    # ── TASK 2 : BashOperator avec Jinja templating ──────────
    # Jinja {{ }} est interprété par Airflow avant l'exécution
    task_jinja_bash = BashOperator(
        task_id      = 'afficher_contexte_jinja',
        bash_command = (
            'echo "=== Contexte Jinja ===" && '
            'echo "Date exec    : {{ ds }}" && '
            'echo "Date nodash  : {{ ds_nodash }}" && '
            'echo "Run ID       : {{ run_id }}" && '
            'echo "DAG ID       : {{ dag.dag_id }}" && '
            'echo "Nb essais    : {{ task_instance.try_number }}" && '
            'echo "Param seuil  : {{ params.seuil_ca }}"'
        ),
    )
    # Variables Jinja disponibles :
    # {{ ds }}          → date exécution YYYY-MM-DD
    # {{ ds_nodash }}   → date sans tirets YYYYMMDD
    # {{ run_id }}      → identifiant unique du run
    # {{ dag.dag_id }}  → identifiant du DAG
    # {{ params.xxx }}  → paramètre passé au déclenchement
    # {{ var.value.xxx }}→ Variable Airflow lue en Jinja
    # {{ task_instance }}→ objet task instance complet
    # {{ macros.ds_add(ds, 7) }} → date + 7 jours

    # ── TASK 3 : Générer des données selon la config ──────────
    @task
    def generer_donnees(config: dict, ds=None):
        import random
        random.seed(int(ds.replace('-', '')))

        produits = ['Laptop Pro','Smartphone X','Tablette Air','Écouteurs BT']
        prix     = {'Laptop Pro':1200,'Smartphone X':650,
                    'Tablette Air':450,'Écouteurs BT':120}

        vendeurs = config.get('vendeurs', ['Alice','Karim'])
        regions  = config.get('regions',  ['Île-de-France'])

        rows = []
        for _ in range(random.randint(15, 30)):
            p   = random.choice(produits)
            qte = random.randint(1, 10)
            rows.append({
                'date'    : ds,
                'produit' : p,
                'vendeur' : random.choice(vendeurs),
                'region'  : random.choice(regions),
                'quantite': qte,
                'montant' : qte * prix[p],
            })

        df   = pd.DataFrame(rows)
        path = f'/tmp/donnees_j6_{ds}.csv'
        df.to_csv(path, index=False)
        print(f"Généré {len(df)} lignes — CA: {df['montant'].sum():.0f}€")
        return path

    # ── TASK 4 : Alerter si CA sous le seuil ─────────────────
    @task
    def alerter_si_seuil(path: str, config: dict):
        df       = pd.read_csv(path)
        ca_total = df['montant'].sum()
        seuil    = config.get('seuil_ca', 5000)

        if ca_total < seuil:
            msg = (f"⚠️ ALERTE : CA {ca_total:.0f}€ < seuil {seuil}€ "
                   f"→ Notification → {config.get('email')}")
            print(msg)
        else:
            print(f"✓ CA {ca_total:.0f}€ > seuil {seuil}€ — RAS")

        return ca_total

    # ── TASK 5 : Sauvegarder avec la config ───────────────────
    @task
    def sauvegarder(path: str, config: dict, ds=None):
        df      = pd.read_csv(path)
        db_path = config.get('db_path', '/tmp/entrepot_j6.db')

        conn = sqlite3.connect(db_path)
        conn.execute("""CREATE TABLE IF NOT EXISTS ventes
                        (date TEXT, produit TEXT, vendeur TEXT,
                         region TEXT, quantite INTEGER, montant REAL)""")
        conn.execute(f"DELETE FROM ventes WHERE date='{ds}'")
        df.to_sql('ventes', conn, if_exists='append', index=False)
        conn.commit()
        count = conn.execute(
            f"SELECT COUNT(*) FROM ventes WHERE date='{ds}'"
        ).fetchone()[0]
        conn.close()
        print(f"Sauvegardé : {count} lignes dans {db_path}")

    # ── Enchaînement ─────────────────────────────────────────
    config = lire_config()
    task_jinja_bash
    donnees = generer_donnees(config)
    alerter_si_seuil(donnees, config)
    sauvegarder(donnees, config)

dag_instance = pipeline_variables()
