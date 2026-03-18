"""
P12 - Sport Data Solution
Génération de données sportives simulées (type Strava).

Deux modes :
- BOOTSTRAP (première exécution) : ~4700 activités historiques avec slack_sent = TRUE
- INCREMENTAL (exécutions suivantes) : 1-3 nouvelles activités avec slack_sent = FALSE
"""

import os
import random
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv
import sys

load_dotenv()

DATE_FIN = datetime(2026, 3, 1)
DATE_DEBUT = datetime(2025, 3, 1)

PROFILS = {
    "Course à pied": {
        "distance": (3000, 20000),
        "vitesse_kmh": (8, 14),
        "heures": [6, 7, 8, 12, 17, 18, 19],
    },
    "Randonnée": {
        "distance": (5000, 25000),
        "vitesse_kmh": (3, 5),
        "heures": [8, 9, 10],
        "jours": [5, 6],
    },
    "Vélo": {
        "distance": (10000, 60000),
        "vitesse_kmh": (18, 30),
        "heures": [7, 8, 9, 17, 18],
    },
    "Natation": {
        "distance": (500, 3000),
        "vitesse_kmh": (2, 4),
        "heures": [7, 8, 12, 18, 19, 20],
    },
    "Tennis": {
        "distance": None,
        "duree_min": (45, 120),
        "heures": [10, 14, 17, 18, 19],
    },
    "Football": {
        "distance": None,
        "duree_min": (60, 120),
        "heures": [18, 19, 20],
        "jours": [1, 3, 5],
    },
    "Rugby": {
        "distance": None,
        "duree_min": (60, 120),
        "heures": [18, 19, 20],
        "jours": [1, 3, 5],
    },
    "Badminton": {
        "distance": None,
        "duree_min": (30, 90),
        "heures": [12, 18, 19, 20],
    },
    "Escalade": {
        "distance": None,
        "duree_min": (60, 180),
        "heures": [10, 14, 17, 18],
    },
    "Yoga": {
        "distance": None,
        "duree_min": (30, 75),
        "heures": [7, 8, 12, 18, 19],
    },
    "Boxe": {
        "distance": None,
        "duree_min": (45, 90),
        "heures": [12, 18, 19, 20],
    },
    "Marche": {
        "distance": (3000, 12000),
        "vitesse_kmh": (4, 6),
        "heures": [8, 9, 12, 17, 18],
    },
}

SPORT_MAPPING = {
    "Runing":          ["Course à pied"],
    "Randonnée":       ["Randonnée", "Marche"],
    "Tennis":          ["Tennis"],
    "Natation":        ["Natation"],
    "Football":        ["Football", "Course à pied"],
    "Rugby":           ["Rugby", "Course à pied"],
    "Badminton":       ["Badminton"],
    "Voile":           ["Marche", "Natation"],
    "Judo":            ["Course à pied", "Yoga"],
    "Boxe":            ["Boxe", "Course à pied"],
    "Escalade":        ["Escalade", "Randonnée"],
    "Triathlon":       ["Course à pied", "Vélo", "Natation"],
    "Équitation":      ["Marche", "Randonnée"],
    "Tennis de table": ["Marche"],
    "Basketball":      ["Course à pied"],
}

COMMENTAIRES = {
    "Course à pied": [
        None, None, None, None, None,
        "Bonne séance ce matin",
        "Nouveau record perso !",
        "Session fractionné",
        "Petit footing tranquille",
        "Reprise après 2 semaines",
    ],
    "Randonnée": [
        None, None, None,
        "Super sentier !",
        "Vue magnifique au sommet",
        "Pic Saint-Loup toujours aussi beau",
        "Randonnée de St Guilhem le desert, je vous la conseille c'est top",
    ],
    "Vélo": [
        None, None, None, None,
        "Sortie en groupe ce matin",
        "Vent de face au retour...",
        "Belle boucle dans les vignes",
    ],
    "default": [
        None, None, None, None, None,
        "Bonne séance !",
        "Super entraînement",
        "Ca fait du bien !",
    ],
}

SAISONNALITE = {
    1: 0.6, 2: 0.7, 3: 0.85, 4: 1.0, 5: 1.15, 6: 1.2,
    7: 1.1, 8: 0.9, 9: 1.05, 10: 0.95, 11: 0.75, 12: 0.6,
}


def generer_activite(id_salarie, sport, date_jour):
    profil = PROFILS.get(sport, PROFILS["Marche"])
    heure = random.choice(profil["heures"])
    minute = random.randint(0, 59)
    date_debut = date_jour.replace(hour=heure, minute=minute, second=0)

    if profil.get("distance"):
        distance_m = random.randint(*profil["distance"])
        vitesse = random.uniform(*profil["vitesse_kmh"])
        duree_s = int((distance_m / 1000) / vitesse * 3600)
    else:
        distance_m = None
        duree_s = random.randint(*profil["duree_min"]) * 60

    date_fin = date_debut + timedelta(seconds=duree_s)
    pool = COMMENTAIRES.get(sport, COMMENTAIRES["default"])
    commentaire = random.choice(pool)

    return (id_salarie, date_debut, sport, distance_m, date_fin, commentaire)


def determine_profil(sport_declare, moyen_deplacement):
    modes_sportifs = ["Marche/running", "Vélo/Trottinette/Autres"]

    if sport_declare:
        sports = list(SPORT_MAPPING.get(sport_declare, ["Course à pied", "Marche"]))
        if random.random() < 0.3:
            extra = random.choice(["Marche", "Yoga", "Vélo"])
            if extra not in sports:
                sports.append(extra)
        return sports, 0.5, 1.5

    elif moyen_deplacement in modes_sportifs:
        if moyen_deplacement == "Marche/running":
            return ["Marche", "Course à pied"], 0, 0.5
        return ["Vélo", "Marche"], 0, 0.5

    else:
        return ["Marche"], 0, 0.1


def generer_tout(cur):
    """Génère ~4700 activités historiques sur 12 mois."""
    cur.execute("""
        SELECT s.id_salarie, s.moyen_deplacement, sd.sport
        FROM raw.salaries s
        LEFT JOIN raw.sports_declares sd ON s.id_salarie = sd.id_salarie
        ORDER BY s.id_salarie
    """)
    salaries = cur.fetchall()
    activites = []

    for id_sal, moyen_dep, sport_decl in salaries:
        sports, freq_min, freq_max = determine_profil(sport_decl, moyen_dep)
        semaine = DATE_DEBUT

        while semaine < DATE_FIN:
            coeff = SAISONNALITE.get(semaine.month, 1.0)
            nb = max(0, round(random.uniform(freq_min, freq_max) * coeff))

            for _ in range(nb):
                sport = random.choice(sports)
                profil = PROFILS.get(sport, PROFILS["Marche"])

                if "jours" in profil and random.random() < 0.7:
                    jour = random.choice(profil["jours"])
                else:
                    jour = random.randint(0, 6)

                date_act = semaine + timedelta(days=jour)
                if date_act >= DATE_FIN:
                    continue
                activites.append(generer_activite(id_sal, sport, date_act))

            semaine += timedelta(weeks=1)

    activites.sort(key=lambda x: x[1])
    return activites


def generer_incrementales(cur):
    """Génère 1-3 nouvelles activités (simulation API Strava)."""
    cur.execute("""
        SELECT s.id_salarie, s.moyen_deplacement, sd.sport
        FROM raw.salaries s
        LEFT JOIN raw.sports_declares sd ON s.id_salarie = sd.id_salarie
        WHERE sd.sport IS NOT NULL
        ORDER BY RANDOM()
        LIMIT 3
    """)
    salaries = cur.fetchall()
    nouvelles = []

    for id_sal, moyen_dep, sport_decl in salaries:
        sports = list(SPORT_MAPPING.get(sport_decl, ["Course à pied", "Marche"]))
        sport = random.choice(sports)
        activite = generer_activite(id_sal, sport, datetime.now())
        nouvelles.append(activite)

    return nouvelles


# --- MAIN ---
conn = None
cur = None

try:
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    cur = conn.cursor()
    print("Connexion OK")

    # Déterminer le mode : bootstrap ou incrémental
    cur.execute("SELECT COUNT(*) FROM raw.activites_sportives")
    count = cur.fetchone()[0]

    if count == 0:
        # ============================================
        # MODE BOOTSTRAP : première exécution
        # Génère l'historique complet, slack_sent = TRUE
        # ============================================
        print("MODE BOOTSTRAP : generation de l'historique...")
        random.seed(42)  # Reproductibilité pour l'historique

        activites = generer_tout(cur)
        print(f"{len(activites)} activites historiques generees")

        cur.execute("TRUNCATE TABLE raw.activites_sportives RESTART IDENTITY;")

        query = """
            INSERT INTO raw.activites_sportives
                (id_salarie, date_debut, type_sport, distance_m, date_fin, commentaire, slack_sent)
            VALUES %s
        """
        # Ajouter slack_sent = True à chaque tuple
        activites_bootstrap = [a + (True,) for a in activites]

        extras.execute_values(cur, query, activites_bootstrap, page_size=1000)
        conn.commit()
        print(f"{len(activites)} activites inserees (slack_sent = TRUE, pas de notification)")

    else:
        # ============================================
        # MODE INCREMENTAL : exécutions suivantes
        # Ajoute 1-3 nouvelles activités, slack_sent = FALSE
        # ============================================
        print(f"MODE INCREMENTAL : {count} activites existantes, ajout de nouvelles...")
        # Pas de seed : on veut des activités différentes à chaque run

        nouvelles = generer_incrementales(cur)

        query = """
            INSERT INTO raw.activites_sportives
                (id_salarie, date_debut, type_sport, distance_m, date_fin, commentaire)
            VALUES %s
        """
        extras.execute_values(cur, query, nouvelles)
        conn.commit()
        print(f"{len(nouvelles)} nouvelles activites inserees (slack_sent = FALSE)")

    # --- Stats communes ---
    cur.execute("SELECT COUNT(*) FROM raw.activites_sportives")
    total = cur.fetchone()[0]

    cur.execute("SELECT COUNT(DISTINCT id_salarie) FROM raw.activites_sportives")
    actifs = cur.fetchone()[0]

    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE slack_sent = TRUE) AS notifiees,
            COUNT(*) FILTER (WHERE slack_sent = FALSE) AS en_attente
        FROM raw.activites_sportives
    """)
    notifiees, en_attente = cur.fetchone()

    print(f"\n  Total activites     : {total}")
    print(f"  Salaries actifs     : {actifs}")
    print(f"  Deja notifiees      : {notifiees}")
    print(f"  En attente Slack    : {en_attente}")

    cur.execute("""
        SELECT type_sport, COUNT(*) FROM raw.activites_sportives
        GROUP BY type_sport ORDER BY COUNT(*) DESC LIMIT 5
    """)
    print("\n  Top 5 sports :")
    for sport, cnt in cur.fetchall():
        print(f"    {sport}: {cnt}")

    cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT id_salarie FROM raw.activites_sportives
            GROUP BY id_salarie HAVING COUNT(*) >= 15
        ) sub
    """)
    print(f"\n  Eligibles jours bien-etre (>= 15 activites) : {cur.fetchone()[0]}")

except Exception as e:
    print(f"ERREUR : {e}")
    if conn:
        conn.rollback()
    sys.exit(1)

finally:
    if cur:
        cur.close()
    if conn:
        conn.close()
    print("Connexion fermee")