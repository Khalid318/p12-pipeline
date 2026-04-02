# P12 - Sport Data Solution

Pipeline de données pour automatiser le calcul d'avantages sportifs (prime + jours de congé) pour les salariés d'une entreprise.

## Le projet

L'entreprise basée à Lattes (34970) veut récompenser ses 161 salariés qui font du sport :

- **Prime sportive** : 5% du salaire brut annuel si le salarié vient au bureau en marchant, courant, à vélo ou en trottinette (distance vérifiée par API Google Maps)
- **5 jours de congé bien-être** : si le salarié a fait au moins 15 activités sportives dans l'année

Tous les seuils (taux de prime, distance max, nb d'activités requis) sont stockés en base et modifiables sans toucher au code.

## Comment ça marche

Le pipeline suit un pattern ELT : les scripts Python chargent les données brutes dans PostgreSQL (schéma `raw`), puis des vues SQL calculent les résultats (schéma `analytics`). Airflow orchestre le tout.

```
Excel RH + Excel Sport
      ↓
  load_data.py → raw.salaries + raw.sports_declares
      ↓
  generate_strava.py → raw.activites_sportives
      ↓
  google_maps.py → raw.validations_transport
      ↓
  run_soda_checks.py → 15 tests de qualité
      ↓
  slack_notify.py → messages Slack
      ↓
  vues analytics → Power BI (DirectQuery)
```

Le premier run génère l'historique complet (~4700 activités). Les runs suivants ajoutent 1-3 nouvelles activités à chaque fois (mode incrémental), et seules les nouvelles sont notifiées sur Slack.

## Stack

- **Airflow** pour l'orchestration (6 tâches, retry automatique, alerte Slack si échec)
- **PostgreSQL** pour le stockage (schéma raw + analytics avec 7 vues)
- **Docker Compose** pour l'infra (5 containers : postgres, airflow-init, webserver, scheduler)
- **SODA Core** pour les tests de qualité (15 checks YAML)
- **Slack Webhooks** pour les notifications
- **Power BI** en DirectQuery pour le dashboard

## Installation

### Prérequis

Docker Desktop, Python 3.12+, Power BI Desktop, Git.

### Setup

```bash
git clone https://github.com/Khalid318/p12-pipeline.git
cd p12-pipeline

cp .env.example .env
# Éditer .env avec vos valeurs
```

Les fichiers `donnees_RH.xlsx` et `donnees_sports.xlsx` dans le dossier `data/` (pas sur GitHub en temps normal, données RH sensibles).

```bash
docker-compose up -d
```

Attendre 3-5 min que tout s'initialise. Vérifier avec `docker ps` (4 containers actifs). 

Airflow sur `localhost:8080` (admin/admin).

### Lancer le pipeline

Dans Airflow : activer le DAG `p12_sport_data_pipeline`, cliquer Play.

Premier run = bootstrap (~4700 activités chargées, dashboard rempli, pas de notification Slack).
Runs suivants = incrémental (1-3 nouvelles activités, notification Slack uniquement pour celles-ci).

## Structure du projet

```
├── docker-compose.yml
├── .env.example
├── sql/
│   └── init.sql                 # 5 tables raw + 7 vues analytics
├── dags/
│   └── dag_p12_pipeline.py      # DAG Airflow (6 tâches)
├── scripts/
│   ├── load_data.py             # Ingestion Excel → PostgreSQL
│   ├── generate_strava.py       # Simulation activités (bootstrap + incrémental)
│   ├── google_maps.py           # Distances domicile-entreprise (mode mock)
│   ├── slack_notify.py          # Notifications Slack
│   └── run_soda_checks.py       # Tests qualité SODA
├── soda/
│   ├── soda_config.yml
│   └── checks.yml               # 15 checks
├── data/                        # Fichiers Excel 
├── notebooks/
│   └── p12_eda.ipynb                # Analyse exploratoire
└── tests/
    ├── test_google_maps.py          # Tests extraction code postal
    ├── test_slack.py                # Tests messages Slack
    └── test_strava.py               # Tests génération activités
```

## Base de données

### Tables (schéma raw)

| Table | Lignes | Contenu |
|-------|--------|---------|
| salaries | 161 | Données RH (nom, salaire, adresse, mode transport) |
| sports_declares | 161 | Sport déclaré par chaque salarié |
| activites_sportives | ~4700+ | Activités simulées type Strava |
| validations_transport | 68 | Distances domicile → Lattes |
| parametres | 6 | Seuils métier modifiables |

### Vues (schéma analytics)

| Vue | Rôle |
|-----|------|
| prime_sportive | Éligibilité + montant de la prime par salarié |
| jours_bien_etre | Éligibilité + jours de congé accordés |
| detail_salarie | Vue consolidée (prime + jours + sport) utilisée par Power BI |
| anomalies_transport | Salariés dont la distance dépasse les seuils |
| controle_qualite | Données manquantes ou incohérentes |
| kpi_global | Chiffres clés en une ligne (pour Airflow/Postgresql) |
| activites_par_mois | Agrégation mensuelle par sport et BU |

Les paramètres métier sont modifiables directement en base :
```sql
UPDATE raw.parametres SET valeur = '0.07' WHERE cle = 'prime_taux';
-- Rafraîchir Power BI → les montants changent, rien d'autre à faire
```

## Tests

### Tests de qualité des données (SODA Core)

15 checks SODA sur les données brutes (doublons, valeurs nulles, distances négatives, salaires réalistes...). Si un check échoue le pipeline s'arrête et une alerte Slack part.

```
[PASS] salaries non vide
[PASS] pas de doublons id_salarie
[PASS] salaire minimum realiste
[FAIL] distances jamais negatives    ← pipeline bloqué, alerte envoyée
```

### Tests unitaires (pytest)

22 tests couvrant les fonctions métier critiques :

```bash
python -m pytest tests/ -v
```

| Fichier | Fonctions testées | Tests |
|---------|-------------------|-------|
| test_google_maps.py | extract_code_postal() | 8 |
| test_slack.py | format_message() | 6 |
| test_strava.py | generer_activite(), determine_profil() | 8 |

Les scripts utilisent un garde `if __name__ == "__main__"` pour séparer la logique métier (importable et testable) de l'exécution pipeline.

## Notifications Slack

Deux types :

**Félicitations** (run incrémental) :
> Bravo Juliette Mendes ! Tu viens de courir 10,8 km en 46 min ! 

**Alertes d'échec** (automatique) :
> PIPELINE EN ÉCHEC — Tâche: ...

Le flag `slack_sent` en base empêche d'envoyer deux fois le même message.

## Dashboard Power BI

**Overview** : 4 KPI (salariés, coût des primes, jours accordés, activités), coût par département, répartition éligibles/non-éligibles, répartition par mode de transport.

Filtres par BU et par sport via une vue dénormalisée (`detail_salarie`) qui consolide primes, jours et sport en une seule table. La page activité utilise `activites_par_mois` avec son propre filtre BU.

## RGPD

- Credentials dans `.env` (hors GitHub)
- Fichiers Excel dans `.gitignore`
- Google Maps ne reçoit que le code postal, pas l'adresse
- Docker isole les services

## Commandes utiles

```bash
docker-compose up -d       # Démarrer
docker-compose down        # Arrêter (données conservées)
docker ps                  # Vérifier les containers
```

---

Projet Data Engineering — OpenClassrooms.
