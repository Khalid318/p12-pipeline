"""
P12 - Sport Data Solution
Génération de données sportives simulées (type Strava).
~4000 activités sur 12 mois, cohérentes avec les profils RH.

Logique :
- Salarié sportif déclaré → 2-5 activités/semaine
- Salarié transport sportif sans sport déclaré → 0-2 activités/semaine
- Salarié sédentaire → 0-1 activité/mois
- Saisonnalité : plus d'activités au printemps/été
"""

import os
import random
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv

load_dotenv()

# Seed pour reproductibilité (même données à chaque run)
random.seed(42)

# Période de génération : 12 derniers mois
DATE_FIN = datetime(2026, 3, 1)
DATE_DEBUT = datetime(2025, 3, 1)

# --- PROFILS SPORTIFS ---
# Pour chaque sport : distances réalistes, vitesses, horaires typiques
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
        "jours": [5, 6],  # weekend
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

# Mapping sport déclaré → types d'activités Strava générées
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

# Commentaires réalistes (majorité None = pas de commentaire)
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

# Saisonnalité : coefficient par mois (plus d'activités au printemps/été)
SAISONNALITE = {
    1: 0.6, 2: 0.7, 3: 0.85, 4: 1.0, 5: 1.15, 6: 1.2,
    7: 1.1, 8: 0.9, 9: 1.05, 10: 0.95, 11: 0.75, 12: 0.6,
}


def generer_activite(id_salarie, sport, date_jour):
    """Génère une activité réaliste pour un salarié et un sport donné."""
    profil = PROFILS.get(sport, PROFILS["Marche"])

    # Heure de début réaliste
    heure = random.choice(profil["heures"])
    minute = random.randint(0, 59)
    date_debut = date_jour.replace(hour=heure, minute=minute, second=0)

    # Distance et durée
    if profil.get("distance"):
        distance_m = random.randint(*profil["distance"])
        vitesse = random.uniform(*profil["vitesse_kmh"])
        duree_s = int((distance_m / 1000) / vitesse * 3600)
    else:
        distance_m = None
        duree_s = random.randint(*profil["duree_min"]) * 60

    date_fin = date_debut + timedelta(seconds=duree_s)

    # Commentaire
    pool = COMMENTAIRES.get(sport, COMMENTAIRES["default"])
    commentaire = random.choice(pool)

    return (id_salarie, date_debut, sport, distance_m, date_fin, commentaire)


def determine_profil(sport_declare, moyen_deplacement):
    """
    Retourne (liste_sports, freq_min_hebdo, freq_max_hebdo) selon le profil du salarié.
    """
    modes_sportifs = ["Marche/running", "Vélo/Trottinette/Autres"]

    if sport_declare:
        # Sportif déclaré : 2-5 activités/semaine
        sports = list(SPORT_MAPPING.get(sport_declare, ["Course à pied", "Marche"]))
        if random.random() < 0.3:
            extra = random.choice(["Marche", "Yoga", "Vélo"])
            if extra not in sports:
                sports.append(extra)
        return sports, 0.5, 1.5

    elif moyen_deplacement in modes_sportifs:
        # Transport sportif sans sport déclaré : occasionnel
        if moyen_deplacement == "Marche/running":
            return ["Marche", "Course à pied"], 0, 0.5
        return ["Vélo", "Marche"], 0, 0.5

    else:
        # Sédentaire : très rare
        return ["Marche"], 0, 0.2


def generer_tout(cur):
    """Génère toutes les activités pour tous les salariés."""
    # Récupérer les profils depuis la base
    cur.execute("""
        SELECT s.id_salarie, s.moyen_deplacement, sd.sport
        FROM salaries s
        LEFT JOIN sports_declares sd ON s.id_salarie = sd.id_salarie
    """)
    salaries = cur.fetchall()

    activites = []

    for id_sal, moyen_dep, sport_decl in salaries:
        sports, freq_min, freq_max = determine_profil(sport_decl, moyen_dep)

        # Parcourir semaine par semaine sur 12 mois
        semaine = DATE_DEBUT
        while semaine < DATE_FIN:
            coeff = SAISONNALITE.get(semaine.month, 1.0)
            nb = max(0, round(random.uniform(freq_min, freq_max) * coeff))

            for _ in range(nb):
                sport = random.choice(sports)
                profil = PROFILS.get(sport, PROFILS["Marche"])

                # Jour dans la semaine (respecter jours préférés si définis)
                if "jours" in profil and random.random() < 0.7:
                    jour = random.choice(profil["jours"])
                else:
                    jour = random.randint(0, 6)

                date_act = semaine + timedelta(days=jour)
                if date_act >= DATE_FIN:
                    continue

                activites.append(generer_activite(id_sal, sport, date_act))

            semaine += timedelta(weeks=1)

    # Trier par date
    activites.sort(key=lambda x: x[1])
    return activites


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

    # Générer les activités
    print("Generation des activites...")
    activites = generer_tout(cur)
    print(f"{len(activites)} activites generees")

    # Full refresh : TRUNCATE + INSERT (données entièrement regénérées à chaque run)
    cur.execute("TRUNCATE TABLE activites_sportives RESTART IDENTITY;")

    query = """
        INSERT INTO activites_sportives
            (id_salarie, date_debut, type_sport, distance_m, date_fin, commentaire)
        VALUES %s
    """

    # Insertion par batch de 1000 (évite de saturer la mémoire sur gros volumes)
    extras.execute_values(cur, query, activites, page_size=1000)

    conn.commit()
    print(f"{len(activites)} activites inserees")

    # Stats rapides
    cur.execute("SELECT COUNT(DISTINCT id_salarie) FROM activites_sportives")
    print(f"Salaries actifs : {cur.fetchone()[0]}")

    cur.execute("""
        SELECT type_sport, COUNT(*) FROM activites_sportives
        GROUP BY type_sport ORDER BY COUNT(*) DESC LIMIT 5
    """)
    print("Top 5 sports :")
    for sport, count in cur.fetchall():
        print(f"  {sport}: {count}")

    cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT id_salarie FROM activites_sportives
            GROUP BY id_salarie HAVING COUNT(*) >= 15
        ) sub
    """)
    print(f"Eligibles jours bien-etre (>= 15 activites) : {cur.fetchone()[0]}")

except Exception as e:
    print(f"ERREUR : {e}")
    if conn:
        conn.rollback()

finally:
    if cur:
        cur.close()
    if conn:
        conn.close()
    print("Connexion fermee")