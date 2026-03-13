-- ============================================================
-- P12 - Sport Data Solution
-- Initialisation de la base sport_data
-- Stack : Airflow + PostgreSQL + Power BI 
-- ============================================================

CREATE DATABASE sport_data;
\c sport_data;

-- Salariés (source : Données_RH.xlsx)
-- NOT NULL sur les champs critiques uniquement (id, nom, salaire)
-- Le reste nullable pour ne pas bloquer le pipeline si un champ RH est vide
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

-- Sports déclarés (source : Données_Sportive.xlsx)
-- PK sur id_salarie : un salarié = une ligne, permet ON CONFLICT
CREATE TABLE IF NOT EXISTS sports_declares (
    id_salarie  INTEGER PRIMARY KEY REFERENCES salaries(id_salarie),
    sport       VARCHAR(100),
    created_at  TIMESTAMP DEFAULT NOW()
);

-- Paramètres métier (modifiables sans toucher au code)
-- Justification : pas de dbt dans la stack, donc les seuils vivent en base
CREATE TABLE IF NOT EXISTS parametres (
    cle         VARCHAR(50) PRIMARY KEY,
    valeur      VARCHAR(100) NOT NULL,
    description TEXT
);

INSERT INTO parametres (cle, valeur, description) VALUES
    ('prime_taux',             '0.05',  'Taux de la prime sportive'),
    ('seuil_activites',        '15',    'Nb min activites pour jours bien-etre'),
    ('nb_jours_bien_etre',     '5',     'Nb jours bien-etre si eligible'),
    ('distance_max_marche_km', '15',    'Distance max marche/running'),
    ('distance_max_velo_km',   '25',    'Distance max velo/trottinette'),
    ('adresse_entreprise',     '1362 Av. des Platanes, 34970 Lattes', 'Adresse de reference')
ON CONFLICT (cle) DO NOTHING;

-- Index sur les colonnes lues par Power BI (jointures et filtres)
-- 161 lignes : le coût en écriture est nul, le gain en lecture est réel
CREATE INDEX IF NOT EXISTS idx_salaries_bu ON salaries(bu);
CREATE INDEX IF NOT EXISTS idx_salaries_transport ON salaries(moyen_deplacement);