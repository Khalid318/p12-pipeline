-- ============================================================
-- P12 - Sport Data Solution
-- Architecture ELT : raw (tables physiques) + analytics (vues)
-- ============================================================

CREATE DATABASE sport_data;
\c sport_data;

-- ============================================================
-- SCHEMA RAW : données brutes ingérées
-- ============================================================

CREATE SCHEMA IF NOT EXISTS raw;

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

CREATE TABLE IF NOT EXISTS raw.sports_declares (
    id_salarie  INTEGER PRIMARY KEY,
    sport       VARCHAR(100),
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.activites_sportives (
    id          SERIAL PRIMARY KEY,
    id_salarie  INTEGER NOT NULL,
    date_debut  TIMESTAMP NOT NULL,
    type_sport  VARCHAR(50) NOT NULL,
    distance_m  INTEGER,
    date_fin    TIMESTAMP NOT NULL,
    commentaire TEXT,
    slack_sent  BOOLEAN DEFAULT FALSE,
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.validations_transport (
    id_salarie      INTEGER PRIMARY KEY,
    distance_km     NUMERIC(8, 2),
    mode_transport  VARCHAR(50),
    date_calcul     TIMESTAMP DEFAULT NOW()
);

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

CREATE INDEX IF NOT EXISTS idx_activites_salarie ON raw.activites_sportives(id_salarie);
CREATE INDEX IF NOT EXISTS idx_activites_date ON raw.activites_sportives(date_debut);

-- ============================================================
-- SCHEMA ANALYTICS : calculs métier (vues, jamais de tables)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS analytics;

-- Vue 1 : Éligibilité à la prime sportive
CREATE OR REPLACE VIEW analytics.prime_sportive AS
SELECT
    s.id_salarie,
    s.nom,
    s.prenom,
    s.bu,
    s.salaire_brut,
    s.moyen_deplacement,
    vt.distance_km,
    CASE
        WHEN s.moyen_deplacement IN ('Marche/running', 'Vélo/Trottinette/Autres')
             AND (vt.distance_km IS NULL OR
                  (s.moyen_deplacement = 'Marche/running'
                   AND vt.distance_km <= (SELECT valeur::NUMERIC FROM raw.parametres WHERE cle = 'distance_max_marche_km'))
                  OR
                  (s.moyen_deplacement = 'Vélo/Trottinette/Autres'
                   AND vt.distance_km <= (SELECT valeur::NUMERIC FROM raw.parametres WHERE cle = 'distance_max_velo_km'))
             )
        THEN 'Éligible'
        ELSE 'Non éligible'
    END AS statut_prime,
    CASE
        WHEN s.moyen_deplacement IN ('Marche/running', 'Vélo/Trottinette/Autres')
        THEN s.salaire_brut * (SELECT valeur::NUMERIC FROM raw.parametres WHERE cle = 'prime_taux')
        ELSE 0
    END AS montant_prime,
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
        THEN 'Éligible'
        ELSE 'Non éligible'
    END AS statut_jours,
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
    (SELECT COUNT(*) FROM analytics.prime_sportive WHERE statut_prime = 'Éligible') AS eligibles_prime,
    (SELECT COALESCE(SUM(montant_prime), 0) FROM analytics.prime_sportive WHERE statut_prime = 'Éligible') AS cout_total_primes,
    (SELECT COUNT(*) FROM analytics.jours_bien_etre WHERE statut_jours = 'Éligible') AS eligibles_jours,
    (SELECT COALESCE(SUM(nb_jours_accordes), 0) FROM analytics.jours_bien_etre) AS total_jours_accordes,
    (SELECT COUNT(*) FROM raw.activites_sportives) AS total_activites,
    (SELECT COUNT(DISTINCT id_salarie) FROM raw.activites_sportives) AS salaries_actifs;

-- Vue 6 : Activités par mois (pour courbe mensuelle Power BI)
CREATE OR REPLACE VIEW analytics.activites_par_mois AS
SELECT
    TO_CHAR(a.date_debut, 'YYYY-MM') AS mois,
    EXTRACT(YEAR FROM a.date_debut) AS annee,
    EXTRACT(MONTH FROM a.date_debut) AS num_mois,
    a.type_sport,
    s.bu,
    COUNT(*) AS nb_activites,
    COUNT(DISTINCT a.id_salarie) AS nb_salaries_actifs,
    ROUND(AVG(a.distance_m), 0) AS distance_moyenne_m
FROM raw.activites_sportives a
JOIN raw.salaries s ON a.id_salarie = s.id_salarie
GROUP BY TO_CHAR(a.date_debut, 'YYYY-MM'), EXTRACT(YEAR FROM a.date_debut), EXTRACT(MONTH FROM a.date_debut), a.type_sport, s.bu
ORDER BY mois;



-- Vue 8 : Dimension id_salarie (pour analyse détaillée par salarié)
CREATE OR REPLACE VIEW analytics.detail_salarie AS
SELECT
    p.id_salarie,
    p.nom,
    p.prenom,
    p.bu,
    p.salaire_brut,
    p.moyen_deplacement,
    p.distance_km,
    p.statut_prime,
    p.montant_prime,
    j.nb_activites,
    j.statut_jours,
    j.nb_jours_accordes,
    sd.sport
FROM analytics.prime_sportive p
JOIN analytics.jours_bien_etre j ON p.id_salarie = j.id_salarie
LEFT JOIN raw.sports_declares sd ON p.id_salarie = sd.id_salarie;