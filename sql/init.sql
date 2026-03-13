-- ============================================================
-- P12 - Sport Data Solution
-- Architecture ELT : raw (tables physiques) + analytics (vues)
-- ============================================================

CREATE DATABASE sport_data;
\c sport_data;

-- ============================================================
-- SCHEMA RAW : données brutes ingérées (tables physiques)
-- Règle : PK obligatoire (pour upsert), NOT NULL uniquement
-- sur les champs utilisés dans les calculs métier
-- ============================================================

CREATE SCHEMA IF NOT EXISTS raw;

-- Salariés (source : Données_RH.xlsx)
CREATE TABLE IF NOT EXISTS raw.salaries (
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
CREATE TABLE IF NOT EXISTS raw.sports_declares (
    id_salarie  INTEGER PRIMARY KEY,
    sport       VARCHAR(100),
    created_at  TIMESTAMP DEFAULT NOW()
);

-- Activités sportives simulées (type Strava)
CREATE TABLE IF NOT EXISTS raw.activites_sportives (
    id          SERIAL PRIMARY KEY,
    id_salarie  INTEGER NOT NULL,
    date_debut  TIMESTAMP NOT NULL,
    type_sport  VARCHAR(50) NOT NULL,
    distance_m  INTEGER,
    date_fin    TIMESTAMP NOT NULL,
    commentaire TEXT,
    created_at  TIMESTAMP DEFAULT NOW()
);

-- Validations transport (résultats API Google Maps)
CREATE TABLE IF NOT EXISTS raw.validations_transport (
    id_salarie      INTEGER PRIMARY KEY,
    distance_km     NUMERIC(8, 2),
    mode_transport  VARCHAR(50),
    date_calcul     TIMESTAMP DEFAULT NOW()
);

-- Paramètres métier (modifiables sans toucher au code)
CREATE TABLE IF NOT EXISTS raw.parametres (
    cle         VARCHAR(50) PRIMARY KEY,
    valeur      VARCHAR(100) NOT NULL,
    description TEXT
);

INSERT INTO raw.parametres (cle, valeur, description) VALUES
    ('prime_taux',             '0.05',  'Taux de la prime sportive'),
    ('seuil_activites',        '15',    'Nb min activites pour jours bien-etre'),
    ('nb_jours_bien_etre',     '5',     'Nb jours bien-etre si eligible'),
    ('distance_max_marche_km', '15',    'Distance max marche/running'),
    ('distance_max_velo_km',   '25',    'Distance max velo/trottinette'),
    ('adresse_entreprise',     '1362 Av. des Platanes, 34970 Lattes', 'Adresse de reference')
ON CONFLICT (cle) DO NOTHING;

-- Index sur les colonnes de jointure et filtres fréquents
CREATE INDEX IF NOT EXISTS idx_activites_salarie ON raw.activites_sportives(id_salarie);
CREATE INDEX IF NOT EXISTS idx_activites_date ON raw.activites_sportives(date_debut);

-- ============================================================
-- SCHEMA ANALYTICS : calculs métier (vues, jamais de tables)
-- Règle : toute logique métier ici, recalculée à chaque lecture
-- Avantage : si un paramètre change, Power BI voit le résultat
-- immédiatement sans relancer de script
-- ============================================================

CREATE SCHEMA IF NOT EXISTS analytics;

-- Vue 1 : Éligibilité à la prime sportive
-- Un salarié est éligible si son mode de transport est sportif
-- ET sa distance domicile-entreprise est dans les seuils
CREATE OR REPLACE VIEW analytics.prime_sportive AS
SELECT
    s.id_salarie,
    s.nom,
    s.prenom,
    s.bu,
    s.salaire_brut,
    s.moyen_deplacement,
    vt.distance_km,
    -- Éligibilité
    CASE
        WHEN s.moyen_deplacement IN ('Marche/running', 'Vélo/Trottinette/Autres')
             AND (vt.distance_km IS NULL OR  -- pas encore validé = éligible par défaut
                  (s.moyen_deplacement = 'Marche/running'
                   AND vt.distance_km <= (SELECT valeur::NUMERIC FROM raw.parametres WHERE cle = 'distance_max_marche_km'))
                  OR
                  (s.moyen_deplacement = 'Vélo/Trottinette/Autres'
                   AND vt.distance_km <= (SELECT valeur::NUMERIC FROM raw.parametres WHERE cle = 'distance_max_velo_km'))
             )
        THEN TRUE
        ELSE FALSE
    END AS eligible,
    -- Montant de la prime
    CASE
        WHEN s.moyen_deplacement IN ('Marche/running', 'Vélo/Trottinette/Autres')
        THEN s.salaire_brut * (SELECT valeur::NUMERIC FROM raw.parametres WHERE cle = 'prime_taux')
        ELSE 0
    END AS montant_prime,
    -- Motif si non éligible
    CASE
        WHEN s.moyen_deplacement NOT IN ('Marche/running', 'Vélo/Trottinette/Autres')
        THEN 'Mode de transport non sportif'
        WHEN vt.distance_km IS NOT NULL
             AND s.moyen_deplacement = 'Marche/running'
             AND vt.distance_km > (SELECT valeur::NUMERIC FROM raw.parametres WHERE cle = 'distance_max_marche_km')
        THEN 'Distance trop élevée pour marche/running (' || vt.distance_km || ' km)'
        WHEN vt.distance_km IS NOT NULL
             AND s.moyen_deplacement = 'Vélo/Trottinette/Autres'
             AND vt.distance_km > (SELECT valeur::NUMERIC FROM raw.parametres WHERE cle = 'distance_max_velo_km')
        THEN 'Distance trop élevée pour vélo/trottinette (' || vt.distance_km || ' km)'
        ELSE NULL
    END AS motif_non_eligible
FROM raw.salaries s
LEFT JOIN raw.validations_transport vt ON s.id_salarie = vt.id_salarie;


-- Vue 2 : Éligibilité aux jours bien-être
-- Un salarié est éligible s'il a >= 15 activités dans l'année
CREATE OR REPLACE VIEW analytics.jours_bien_etre AS
SELECT
    s.id_salarie,
    s.nom,
    s.prenom,
    s.bu,
    COALESCE(a.nb_activites, 0) AS nb_activites,
    (SELECT valeur::INTEGER FROM raw.parametres WHERE cle = 'seuil_activites') AS seuil_requis,
    CASE
        WHEN COALESCE(a.nb_activites, 0) >= (SELECT valeur::INTEGER FROM raw.parametres WHERE cle = 'seuil_activites')
        THEN TRUE
        ELSE FALSE
    END AS eligible,
    CASE
        WHEN COALESCE(a.nb_activites, 0) >= (SELECT valeur::INTEGER FROM raw.parametres WHERE cle = 'seuil_activites')
        THEN (SELECT valeur::INTEGER FROM raw.parametres WHERE cle = 'nb_jours_bien_etre')
        ELSE 0
    END AS nb_jours_accordes
FROM raw.salaries s
LEFT JOIN (
    SELECT id_salarie, COUNT(*) AS nb_activites
    FROM raw.activites_sportives
    GROUP BY id_salarie
) a ON s.id_salarie = a.id_salarie;


-- Vue 3 : Anomalies de déclaration transport
-- Signale les salariés dont la distance dépasse les seuils
CREATE OR REPLACE VIEW analytics.anomalies_transport AS
SELECT
    s.id_salarie,
    s.nom,
    s.prenom,
    s.moyen_deplacement,
    s.adresse_domicile,
    vt.distance_km,
    CASE
        WHEN s.moyen_deplacement = 'Marche/running'
             AND vt.distance_km > (SELECT valeur::NUMERIC FROM raw.parametres WHERE cle = 'distance_max_marche_km')
        THEN 'Distance ' || vt.distance_km || ' km > seuil 15 km pour marche'
        WHEN s.moyen_deplacement = 'Vélo/Trottinette/Autres'
             AND vt.distance_km > (SELECT valeur::NUMERIC FROM raw.parametres WHERE cle = 'distance_max_velo_km')
        THEN 'Distance ' || vt.distance_km || ' km > seuil 25 km pour vélo'
        ELSE NULL
    END AS anomalie
FROM raw.salaries s
JOIN raw.validations_transport vt ON s.id_salarie = vt.id_salarie
WHERE
    (s.moyen_deplacement = 'Marche/running'
     AND vt.distance_km > (SELECT valeur::NUMERIC FROM raw.parametres WHERE cle = 'distance_max_marche_km'))
    OR
    (s.moyen_deplacement = 'Vélo/Trottinette/Autres'
     AND vt.distance_km > (SELECT valeur::NUMERIC FROM raw.parametres WHERE cle = 'distance_max_velo_km'));


-- Vue 4 : Contrôle qualité des données brutes
-- Signale les problèmes dans les données ingérées
CREATE OR REPLACE VIEW analytics.controle_qualite AS
SELECT 'salaire_manquant' AS type_anomalie, id_salarie, nom, prenom
FROM raw.salaries WHERE salaire_brut IS NULL
UNION ALL
SELECT 'transport_manquant', id_salarie, nom, prenom
FROM raw.salaries WHERE moyen_deplacement IS NULL
UNION ALL
SELECT 'adresse_manquante', id_salarie, nom, prenom
FROM raw.salaries WHERE adresse_domicile IS NULL OR adresse_domicile = ''
UNION ALL
SELECT 'activite_sans_salarie', a.id_salarie, NULL, NULL
FROM raw.activites_sportives a
LEFT JOIN raw.salaries s ON a.id_salarie = s.id_salarie
WHERE s.id_salarie IS NULL;


-- Vue 5 : KPI globaux pour Power BI
CREATE OR REPLACE VIEW analytics.kpi_global AS
SELECT
    (SELECT COUNT(*) FROM raw.salaries) AS total_salaries,
    (SELECT COUNT(*) FROM analytics.prime_sportive WHERE eligible = TRUE) AS eligibles_prime,
    (SELECT COALESCE(SUM(montant_prime), 0) FROM analytics.prime_sportive WHERE eligible = TRUE) AS cout_total_primes,
    (SELECT COUNT(*) FROM analytics.jours_bien_etre WHERE eligible = TRUE) AS eligibles_jours,
    (SELECT COALESCE(SUM(nb_jours_accordes), 0) FROM analytics.jours_bien_etre) AS total_jours_accordes,
    (SELECT COUNT(*) FROM raw.activites_sportives) AS total_activites,
    (SELECT COUNT(DISTINCT id_salarie) FROM raw.activites_sportives) AS salaries_actifs;