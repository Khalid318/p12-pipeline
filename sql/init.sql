-- Création de la base sport_data séparée d'Airflow
CREATE DATABASE sport_data;

-- Connexion à sport_data
\c sport_data;

-- Table des salariés (source : Données_RH.xlsx)
CREATE TABLE IF NOT EXISTS salaries (
    id_salarie        INTEGER PRIMARY KEY,
    nom               VARCHAR(100) NOT NULL,
    prenom            VARCHAR(100) NOT NULL,
    date_naissance    DATE,
    bu                VARCHAR(50),
    date_embauche     DATE,
    salaire_brut      NUMERIC(10, 2) NOT NULL,
    type_contrat      VARCHAR(10),
    nb_jours_cp       INTEGER,
    adresse_domicile  TEXT,
    moyen_deplacement VARCHAR(50),
    created_at        TIMESTAMP DEFAULT NOW()
);

-- Table des sports déclarés (source : Données_Sportive.xlsx)
CREATE TABLE IF NOT EXISTS sports_declares (
    id          SERIAL PRIMARY KEY,
    id_salarie  INTEGER REFERENCES salaries(id_salarie),
    sport       VARCHAR(100) DEFAULT 'Aucun',
    created_at  TIMESTAMP DEFAULT NOW()
);