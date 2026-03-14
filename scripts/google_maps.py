"""
P12 - Sport Data Solution
Calcul des distances domicile → entreprise via API Google Maps Distance Matrix.

RGPD : on envoie uniquement le CODE POSTAL à Google Maps, pas l'adresse complète.
C'est suffisant pour détecter les anomalies (seuils de 15 et 25 km)
tout en respectant la minimisation des données.

Mode mock : si GOOGLE_MAPS_API_KEY n'est pas défini, génère des distances
simulées à partir des codes postaux (pour développement/test sans API).
"""

import os
import re
import random
import psycopg2
from psycopg2 import extras
import requests
from dotenv import load_dotenv

load_dotenv()

# Adresse de l'entreprise (destination fixe)
ADRESSE_ENTREPRISE = "34970 Lattes, France"

# Distances simulées par code postal (en km) — pour le mode mock
# Estimations réalistes des distances jusqu'à Lattes
DISTANCES_MOCK = {
    "34000": 5.0,    # Montpellier centre
    "34070": 6.5,    # Montpellier sud
    "34080": 4.0,    # Montpellier est
    "34090": 7.0,    # Montpellier nord
    "34170": 8.5,    # Castelnau-le-Lez
    "34470": 12.0,   # Pérols
    "34970": 1.5,    # Lattes (même ville)
    "34130": 10.0,   # Mauguio
    "34400": 15.0,   # Lunel
    "34200": 8.0,    # Sète
    "34500": 25.0,   # Béziers
    "30000": 55.0,   # Nîmes
    "30900": 55.0,   # Nîmes
    "34700": 30.0,   # Lodève
}


def extract_code_postal(adresse):
    """
    Extrait le code postal d'une adresse française.
    Ex: '128 Rue du Port, 34000 Frontignan' → '34000'
    """
    if not adresse:
        return None
    match = re.search(r'\b(\d{5})\b', str(adresse))
    return match.group(1) if match else None


def get_distance_google(code_postal, api_key):
    """
    Appelle l'API Google Maps Distance Matrix.
    Envoie uniquement le code postal (RGPD : minimisation des données).
    Retourne la distance en km ou None si erreur.
    """
    origin = f"{code_postal}, France"
    url = "https://maps.googleapis.com/maps/api/distancematrix/json"
    params = {
        "origins": origin,
        "destinations": ADRESSE_ENTREPRISE,
        "mode": "driving",  # distance routière (pas vol d'oiseau)
        "language": "fr",
        "key": api_key,
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        data = response.json()

        if data["status"] != "OK":
            print(f"  API erreur globale: {data['status']}")
            return None

        element = data["rows"][0]["elements"][0]
        if element["status"] != "OK":
            print(f"  API erreur pour {code_postal}: {element['status']}")
            return None

        distance_m = element["distance"]["value"]
        return round(distance_m / 1000, 2)

    except Exception as e:
        print(f"  Erreur API pour {code_postal}: {e}")
        return None


def get_distance_mock(code_postal):
    """
    Mode développement : retourne une distance simulée basée sur le code postal.
    Ajoute un peu de bruit aléatoire pour le réalisme.
    """
    base = DISTANCES_MOCK.get(code_postal)
    if base:
        # Ajoute +/- 20% de bruit
        noise = random.uniform(-0.2, 0.2) * base
        return round(base + noise, 2)

    # Code postal inconnu : distance basée sur le département
    dept = code_postal[:2] if code_postal else None
    if dept == "34":
        return round(random.uniform(5, 35), 2)    # Hérault
    elif dept == "30":
        return round(random.uniform(40, 70), 2)    # Gard
    elif dept == "11":
        return round(random.uniform(80, 120), 2)   # Aude
    else:
        return round(random.uniform(50, 150), 2)    # Autre département


# --- MAIN ---
conn = None
cur = None

try:
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )
    cur = conn.cursor()
    print("Connexion OK")

    # Détecter le mode (API réelle ou mock)
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    mock_mode = not api_key
    if mock_mode:
        print("MODE MOCK : pas de clé API Google Maps, distances simulées")
        random.seed(42)
    else:
        print("MODE API : utilisation de Google Maps Distance Matrix")

    # Récupérer les salariés avec mode de transport sportif
    # (les seuls pour qui la distance est pertinente)
    cur.execute("""
        SELECT id_salarie, adresse_domicile, moyen_deplacement
        FROM raw.salaries
        WHERE moyen_deplacement IN ('Marche/running', 'Vélo/Trottinette/Autres')
    """)
    salaries = cur.fetchall()
    print(f"{len(salaries)} salaries avec transport sportif a valider")

    records = []
    for id_sal, adresse, mode_transport in salaries:
        code_postal = extract_code_postal(adresse)

        if not code_postal:
            print(f"  Salarie {id_sal}: pas de code postal dans '{adresse}'")
            records.append((id_sal, None, mode_transport))
            continue

        if mock_mode:
            distance = get_distance_mock(code_postal)
        else:
            distance = get_distance_google(code_postal, api_key)

        records.append((id_sal, distance, mode_transport))

    # Insertion avec upsert (idempotent)
    query = """
        INSERT INTO raw.validations_transport (id_salarie, distance_km, mode_transport)
        VALUES %s
        ON CONFLICT (id_salarie) DO UPDATE SET
            distance_km = EXCLUDED.distance_km,
            mode_transport = EXCLUDED.mode_transport,
            date_calcul = NOW()
    """

    extras.execute_values(cur, query, records)
    conn.commit()
    print(f"{len(records)} validations inserees dans raw.validations_transport")

    # Stats
    cur.execute("""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE distance_km <= 15) AS dans_seuil_marche,
            COUNT(*) FILTER (WHERE distance_km <= 25) AS dans_seuil_velo,
            COUNT(*) FILTER (WHERE distance_km > 25) AS hors_seuil,
            ROUND(AVG(distance_km), 1) AS distance_moyenne
        FROM raw.validations_transport
    """)
    row = cur.fetchone()
    print(f"\n  Total validations    : {row[0]}")
    print(f"  Distance <= 15 km   : {row[1]}")
    print(f"  Distance <= 25 km   : {row[2]}")
    print(f"  Distance > 25 km    : {row[3]}")
    print(f"  Distance moyenne     : {row[4]} km")

    # Afficher les anomalies
    cur.execute("SELECT * FROM analytics.anomalies_transport")
    anomalies = cur.fetchall()
    if anomalies:
        print(f"\n  {len(anomalies)} ANOMALIES detectees :")
        for a in anomalies[:5]:
            print(f"    Salarie {a[0]} ({a[1]} {a[2]}): {a[3]} - {a[4]} km - {a[5]}")
    else:
        print("\n  Aucune anomalie detectee")

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
