# 🔄 10 Days of ETL/Airflow — Série Complète

> **Série de portfolio** · 10 projets · Apache Airflow 2.8+  
> Orchestration de pipelines de données, du débutant au projet final complet

---

## 📁 Structure du dépôt

```
10-days-airflow/
│
├── day-01-introduction/
│   ├── dag_hello_world.py       ← 4 tasks enchaînées, BashOperator + PythonOperator
│   ├── dag_etl_simple.py        ← Pattern Extract → Transform → Load
│   └── README.md
│
├── day-02-operators-avances/
│   ├── dag_branch_operator.py   ← Pipeline conditionnel, 3 branches
│   ├── dag_file_sensor.py       ← Attendre un fichier avec FileSensor
│   ├── dag_dependencies.py      ← Parallélisme + convergence + trigger_rule
│   └── README.md
│
├── day-03-connections-hooks/
│   ├── dag_connections_hooks.py ← ETL complet avec SQLite Hook
│   ├── dag_http_hook.py         ← Appel API REST via HttpHook
│   └── README.md
│
├── day-04-taskflow/
│   ├── dag_taskflow_intro.py    ← @task, @dag, XCom automatique
│   ├── dag_taskflow_avance.py   ← @task_group, multiple_outputs
│   └── README.md
│
├── day-05-pipeline-complet/
│   ├── dag_pipeline_complet.py  ← Idempotence, Data Quality, callbacks
│   └── README.md
│
├── day-06-dynamic-variables/
│   ├── dag_variables_jinja.py   ← Variables Airflow + Jinja templating
│   ├── dag_dynamic.py           ← 3 DAGs générés dynamiquement
│   └── README.md
│
├── day-07-sources-externes/
│   ├── dag_sources_externes.py  ← API REST + fichiers + fallback + consolidation
│   └── README.md
│
├── day-08-monitoring/
│   ├── dag_monitoring.py        ← SLA, callbacks, métriques, anomaly detection
│   └── README.md
│
├── day-09-tests/
│   ├── dag_a_tester.py          ← DAG avec fonctions pures testables
│   ├── test_dag.py              ← 13 tests pytest (unitaires + intégration)
│   └── README.md
│
├── day-10-projet-final/
│   ├── dag_final_etl.py         ← Tout combiné en un pipeline professionnel
│   └── README.md
│
└── README.md                    ← Ce fichier
```

---

## 🚀 Installation rapide

### Option A — Docker (recommandée)

```bash
# Télécharger le docker-compose officiel
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.8.1/docker-compose.yaml'

# Créer les dossiers
mkdir -p ./dags ./logs ./plugins ./config

# Copier tous les DAGs
cp day-*/dag_*.py ./dags/

# Lancer
docker-compose up -d

# Interface : http://localhost:8080
# Login : airflow / airflow
```

### Option B — Installation locale

```bash
python -m venv airflow_env
source airflow_env/bin/activate           # Linux/Mac
# airflow_env\Scripts\activate            # Windows

pip install apache-airflow==2.8.1 \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.11.txt"

pip install pandas pytest

export AIRFLOW_HOME=~/airflow
airflow db init

airflow users create \
    --username admin --firstname Admin \
    --lastname User --role Admin \
    --email admin@example.com --password admin

# Terminal 1
airflow webserver --port 8080

# Terminal 2
airflow scheduler

# Copier les DAGs
cp day-*/dag_*.py ~/airflow/dags/
```

---

## 🗓️ Les 10 projets

### Jour 1 — Introduction
**Fichiers :** `dag_hello_world.py` · `dag_etl_simple.py`

Concepts fondamentaux d'Airflow. Un DAG en 4 tasks enchaînées avec BashOperator et PythonOperator. Communication entre tasks via XCom. Premier pipeline Extract → Transform → Load.

```python
dag = DAG(dag_id='jour1_hello_world', schedule='@daily')
task_bash >> task_python >> task_verifier >> task_fin
```

```bash
airflow dags trigger jour1_hello_world
airflow dags trigger jour1_etl_simple
```

---

### Jour 2 — Operators Avancés
**Fichiers :** `dag_branch_operator.py` · `dag_file_sensor.py` · `dag_dependencies.py`

BranchPythonOperator pour les pipelines conditionnels. FileSensor pour attendre un fichier. Tasks en parallèle avec convergence et `trigger_rule`.

```python
task_branch >> [task_gros, task_moyen, task_faible]
[task_gros, task_moyen, task_faible] >> task_fin  # trigger_rule='none_failed_min_one_success'
```

```bash
airflow dags trigger jour2_branch_operator
airflow dags trigger jour2_file_sensor
airflow dags trigger jour2_dependances
```

---

### Jour 3 — Connexions & Hooks
**Fichiers :** `dag_connections_hooks.py` · `dag_http_hook.py`

Stocker les credentials dans Airflow (pas dans le code). SQLite Hook pour les opérations en base. HTTP Hook pour appeler une API REST.

```bash
# Créer les connexions avant de lancer
airflow connections add 'sqlite_entrepot' --conn-type sqlite --conn-host '/tmp/entrepot_j3.db'
airflow connections add 'api_jsonplaceholder' --conn-type http --conn-host 'https://jsonplaceholder.typicode.com'

airflow dags trigger jour3_connections_hooks
airflow dags trigger jour3_http_hook
```

---

### Jour 4 — TaskFlow API
**Fichiers :** `dag_taskflow_intro.py` · `dag_taskflow_avance.py`

Syntaxe moderne Airflow 2.0+. `@task` remplace PythonOperator. XCom automatique via les arguments. `@task_group` pour regrouper visuellement. `multiple_outputs=True` pour retourner plusieurs valeurs.

```python
@dag(dag_id='jour4_taskflow_intro', schedule='@daily')
def pipeline():
    raw   = extraire()
    clean = transformer(raw)    # dépendance inférée automatiquement
    charger(clean)

dag_instance = pipeline()
```

```bash
airflow dags trigger jour4_taskflow_intro
airflow dags trigger jour4_taskflow_avance
```

---

### Jour 5 — Pipeline Complet
**Fichier :** `dag_pipeline_complet.py`

Pipeline ETL robuste avec idempotence (DELETE avant INSERT), Data Quality en 4 checks, callbacks on_failure/on_retry/on_success, table de KPIs journaliers, rapport cumulé.

```python
# Idempotence
conn.execute(f"DELETE FROM ventes WHERE date = '{ds}'")
df.to_sql('ventes', conn, if_exists='append')

# Data Quality
if erreurs:
    raise ValueError(f"Validation échouée : {erreurs}")
```

```bash
airflow dags trigger jour5_pipeline_complet
sqlite3 /tmp/entrepot_complet.db "SELECT * FROM kpis_journaliers;"
```

---

### Jour 6 — Variables & Dynamic DAGs
**Fichiers :** `dag_variables_jinja.py` · `dag_dynamic.py`

Variables Airflow pour stocker la config hors du code. Jinja templating `{{ ds }}`, `{{ params.xxx }}`, `{{ var.value.xxx }}`. Génération de 3 DAGs depuis une boucle sur une config.

```python
# Variables
env    = Variable.get('environnement', default_var='dev')
config = Variable.get('config_pipeline', deserialize_json=True)

# Dynamic DAGs
for cfg in REGIONS_CONFIG:
    globals()[f'dag_{cfg["code"]}'] = creer_dag_region(cfg)
```

```bash
airflow variables set environnement prod
airflow dags trigger jour6_variables_jinja
airflow dags trigger jour6_region_idf
```

---

### Jour 7 — Sources Externes
**Fichier :** `dag_sources_externes.py`

API REST avec timeout, retry exponentiel et fallback local. Lecture de plusieurs fichiers CSV d'un dossier. Consolidation multi-sources avec `trigger_rule='all_done'`.

```python
default_args = {
    'retries'                   : 3,
    'retry_delay'               : timedelta(minutes=2),
    'retry_exponential_backoff' : True,  # 2min → 4min → 8min
}
```

```bash
airflow dags trigger jour7_sources_externes
sqlite3 /tmp/multi_sources.db "SELECT * FROM consolide;"
```

---

### Jour 8 — Monitoring & Alertes
**Fichier :** `dag_monitoring.py`

SLA par task et global (`dagrun_timeout`). 4 callbacks : `on_failure`, `on_retry`, `on_success`, `sla_miss`. Mesure des durées dans le code. Détection d'anomalies (performance, volume).

```python
@dag(
    dagrun_timeout      = timedelta(minutes=30),
    on_success_callback = on_success_callback,
    sla_miss_callback   = sla_miss_callback,
    default_args = {
        'sla'                : timedelta(minutes=10),
        'on_failure_callback': on_failure_callback,
    }
)
```

```bash
airflow dags trigger jour8_monitoring
sqlite3 /tmp/monitoring_j8.db "SELECT * FROM metriques_pipeline;"
```

---

### Jour 9 — Tests & CI/CD
**Fichiers :** `dag_a_tester.py` · `test_dag.py`

Isoler la logique métier dans des fonctions pures. 13 tests pytest : structure DAG, unitaires, intégration. Fixtures pour les données de test. Intégration GitHub Actions.

```bash
# Lancer les tests
pytest test_dag.py -v

# Résultat attendu
# 13 passed in 0.42s
```

---

### Jour 10 — Projet Final
**Fichier :** `dag_final_etl.py`

Tous les concepts réunis. `@task_group` pour l'extraction parallèle multi-sources. Branchement conditionnel selon le volume. Idempotence complète. Fonctions pures testables. Callbacks de monitoring. Variables Airflow. KPIs journaliers avec détection d'anomalies.

```bash
airflow dags trigger jour10_pipeline_final \
    --conf '{"objectif_ca": 80000, "min_lignes": 15}'

sqlite3 /tmp/final_j10.db "SELECT * FROM kpis_journaliers ORDER BY date DESC;"
```

---

## 🔑 Commandes Airflow essentielles

```bash
# DAGs
airflow dags list                          # lister les DAGs
airflow dags trigger <dag_id>              # déclencher manuellement
airflow dags pause   <dag_id>              # mettre en pause
airflow dags unpause <dag_id>              # reprendre

# Tasks
airflow tasks list <dag_id>                # lister les tasks
airflow tasks test <dag_id> <task_id> <date>  # tester une task seule
airflow tasks logs <dag_id> <task_id> <date>  # voir les logs

# Variables
airflow variables set <key> <value>        # créer/modifier
airflow variables get <key>               # lire
airflow variables list                    # lister

# Connexions
airflow connections add <conn_id> ...      # créer
airflow connections list                  # lister
```

---

## 🧩 Concepts clés résumés

| Concept | Description | Jour |
|---------|-------------|------|
| `DAG` | Graphe orienté acyclique — le pipeline | J1 |
| `Task` | Une étape du pipeline | J1 |
| `XCom` | Communication entre tasks (retour de valeur) | J1 |
| `BranchOperator` | Choisir dynamiquement quelle branche exécuter | J2 |
| `Sensor` | Attendre une condition avant de continuer | J2 |
| `trigger_rule` | Quand une task doit s'exécuter selon ses parents | J2 |
| `Hook` | Classe Python pour se connecter à un système externe | J3 |
| `Connection` | Credentials stockés dans Airflow | J3 |
| `@task` | Décorateur TaskFlow — remplace PythonOperator | J4 |
| `@task_group` | Regrouper des tasks visuellement | J4 |
| `Idempotence` | Pipeline rejouable sans doublons | J5 |
| `Variable` | Config stockée dans Airflow, modifiable sans redéployer | J6 |
| `Jinja {{ }}` | Templates dans bash_command, sql... | J6 |
| `Dynamic DAG` | Générer N DAGs depuis une boucle | J6 |
| `retry_exponential_backoff` | Retry avec délai croissant | J7 |
| `SLA` | Délai maximum d'exécution d'une task | J8 |
| `Callbacks` | Fonctions appelées sur failure/retry/success | J8 |
| `Fonctions pures` | Testables avec pytest sans Airflow | J9 |


---

⭐ **Si ce projet t'aide, mets une étoile !**
