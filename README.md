# 🔌 Jour 3 / 10 — ETL/Airflow : Connexions & Hooks

> **Série : 10 Days of ETL/Airflow** · Jour 3/10  
> Concepts : Connections · Hooks · SQLiteHook · PostgresHook · HttpHook · Admin UI

---

## 📁 Fichiers du projet

```
day-03-connections-hooks/
│
├── dag_connections_hooks.py    ← Pipeline ETL complet avec SQLite
├── dag_http_hook.py            ← Appel API REST via HttpHook
└── README.md
```

---

## 🧠 Connection vs Hook vs Operator

```
Connection = les credentials (host, login, password, port...)
             stockés de façon sécurisée dans Airflow

Hook       = la classe Python qui utilise une Connection
             pour interagir avec un système externe

Operator   = utilise un Hook pour exécuter une action précise
```

```
UI Airflow (Connection) → Hook (se connecte) → Operator (exécute)
```

---

## 🚀 Déployer les DAGs

```bash
# Locale
cp dag_connections_hooks.py ~/airflow/dags/
cp dag_http_hook.py         ~/airflow/dags/

# Docker
cp dag_connections_hooks.py ./dags/
cp dag_http_hook.py         ./dags/
```

---

## 🔑 ÉTAPE 1 — Configurer une connexion dans l'UI

```
http://localhost:8080
→ Admin → Connexions → bouton "+"

Connexion SQLite :
    Conn ID   : sqlite_entrepot
    Conn Type : SQLite
    Host      : /tmp/entrepot_ventes.db

Connexion HTTP (pour l'API) :
    Conn ID   : api_jsonplaceholder
    Conn Type : HTTP
    Host      : https://jsonplaceholder.typicode.com

Connexion PostgreSQL (exemple production) :
    Conn ID   : postgres_prod
    Conn Type : PostgreSQL
    Host      : localhost
    Schema    : ma_base
    Login     : mon_user
    Password  : mon_password
    Port      : 5432
```

---

## 🔑 ÉTAPE 2 — Configurer une connexion en ligne de commande

```bash
# SQLite
airflow connections add 'sqlite_entrepot' \
    --conn-type 'sqlite' \
    --conn-host '/tmp/entrepot_ventes.db'

# HTTP
airflow connections add 'api_jsonplaceholder' \
    --conn-type 'http' \
    --conn-host 'https://jsonplaceholder.typicode.com'

# PostgreSQL
airflow connections add 'postgres_prod' \
    --conn-type 'postgres' \
    --conn-host 'localhost' \
    --conn-schema 'ma_base' \
    --conn-login 'mon_user' \
    --conn-password 'mon_password' \
    --conn-port 5432

# Lister les connexions
airflow connections list
```

---

## 🔑 ÉTAPE 3 — Utiliser les Hooks dans le code

### SQLite Hook

```python
import sqlite3

# Direct (sans connexion Airflow configurée)
conn = sqlite3.connect('/tmp/entrepot_ventes.db')
df.to_sql('ventes', conn, if_exists='append', index=False)
conn.close()
```

### PostgreSQL Hook (production)

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

hook = PostgresHook(postgres_conn_id='postgres_prod')
conn = hook.get_conn()
cursor = conn.cursor()

# Exécuter une requête
cursor.execute("INSERT INTO ventes VALUES (%s, %s)", (val1, val2))
conn.commit()

# Lire avec pandas
df = hook.get_pandas_df("SELECT * FROM ventes WHERE date = %s", parameters=[date])
```

### HTTP Hook

```python
from airflow.providers.http.hooks.http import HttpHook

hook     = HttpHook(method='GET', http_conn_id='api_jsonplaceholder')
response = hook.run('/posts?_limit=10')
data     = response.json()
```

---

## 🔑 ÉTAPE 4 — Comprendre dag_connections_hooks.py

```
Pipeline en 5 étapes :

1. init_base_sqlite   → crée la table si elle n'existe pas
2. extraire_donnees   → simule une extraction (API, fichier, BDD)
3. charger_sqlite     → charge dans SQLite via connexion
4. generer_rapport    → requête SQL + rapport agrégé
5. verification_finale→ comptage des lignes chargées
```

**Commande pour exécuter :**

```bash
airflow dags trigger jour3_connections_hooks

# Voir le résultat en base
sqlite3 /tmp/entrepot_ventes.db "SELECT * FROM ventes LIMIT 5;"
sqlite3 /tmp/entrepot_ventes.db "SELECT vendeur, SUM(montant) FROM ventes GROUP BY vendeur;"
```

---

## 🔑 ÉTAPE 5 — Comprendre dag_http_hook.py

```
AVANT d'exécuter :
→ Configurer la connexion api_jsonplaceholder (étape 2)

Pipeline :
1. appeler_api      → HttpHook appelle l'API, sauvegarde JSON
2. traiter_resultats→ lit le fichier, affiche les résultats
```

```bash
airflow dags trigger jour3_http_hook
```

---

## 🔑 Hooks disponibles selon l'installation

| Hook | Package | Usage |
|------|---------|-------|
| `PostgresHook` | `apache-airflow-providers-postgres` | PostgreSQL |
| `MySqlHook` | `apache-airflow-providers-mysql` | MySQL/MariaDB |
| `HttpHook` | `apache-airflow-providers-http` | API REST |
| `S3Hook` | `apache-airflow-providers-amazon` | AWS S3 |
| `GCSHook` | `apache-airflow-providers-google` | Google Cloud Storage |
| `SFTPHook` | `apache-airflow-providers-sftp` | Serveur SFTP |

### Installer un provider

```bash
pip install apache-airflow-providers-postgres
pip install apache-airflow-providers-amazon
pip install apache-airflow-providers-google
```

---

## 💡 Bonnes pratiques

| Pratique | Pourquoi |
|----------|----------|
| Stocker les credentials dans les Connections | Jamais en dur dans le code |
| Utiliser les Hooks plutôt que les libs directes | Centralise la gestion des connexions |
| Nommer les connexions clairement | `postgres_prod` > `conn1` |
| Tester les connexions dans l'UI | Admin → Connexions → Tester |



---

⭐ **Si ce projet t'aide, mets une étoile !**
