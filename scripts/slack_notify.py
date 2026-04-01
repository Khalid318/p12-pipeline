"""
P12 - Sport Data Solution
Envoi de messages Slack de félicitations pour les activités sportives.
Format conforme aux exemples de la note de cadrage.

Mode mock : si SLACK_WEBHOOK_URL n'est pas défini, affiche les messages
dans la console au lieu de les envoyer (pour développement/test).
"""

import os
import json
import psycopg2
import requests
from dotenv import load_dotenv
import sys

load_dotenv()

# Templates de messages (variés pour éviter la répétition)
TEMPLATES = {
    "Course à pied": [
        "Bravo {prenom} {nom} ! Tu viens de courir {distance} km en {duree} min ! Quelle énergie ! 🔥🏅",
        "Superbe {prenom} {nom} ! {distance} km de course aujourd'hui ! Continue comme ça ! 🏃💪",
    ],
    "Randonnée": [
        "Magnifique {prenom} {nom} ! Une randonnée de {distance} km terminée et un nouveau spot à découvrir ! 🌄🏕",
        "Bravo {prenom} {nom} ! {distance} km de rando, les jambes doivent chauffer ! 🥾⛰️",
    ],
    "Vélo": [
        "Chapeau {prenom} {nom} ! {distance} km à vélo aujourd'hui ! 🚴‍♂️💨",
        "Impressionnant {prenom} {nom} ! {distance} km de vélo, quelle machine ! 🚲🔥",
    ],
    "Natation": [
        "Bravo {prenom} {nom} ! {distance} m dans l'eau aujourd'hui ! 🏊‍♂️💦",
        "Superbe {prenom} {nom} ! {distance} m de nage, un vrai poisson ! 🐟🏊",
    ],
    "default": [
        "Bravo {prenom} {nom} ! Séance de {sport} terminée en {duree} min ! 💪🎉",
        "Superbe {prenom} {nom} ! Encore une séance de {sport}, continue ! 🔥👏",
    ],
}


def format_message(prenom, nom, sport, distance_m, duree_s, commentaire):
    """Génère un message de félicitation personnalisé."""
    import random

    templates = TEMPLATES.get(sport, TEMPLATES["default"])
    template = random.choice(templates)

    # Formatage distance
    if distance_m:
        if sport == "Natation":
            distance_str = str(distance_m)
        else:
            distance_str = str(round(distance_m / 1000, 1))
    else:
        distance_str = ""

    # Formatage durée
    duree_min = round(duree_s / 60) if duree_s else 0

    msg = template.format(
        prenom=prenom,
        nom=nom,
        sport=sport,
        distance=distance_str,
        duree=duree_min,
    )

    # Ajouter le commentaire s'il existe (comme dans l'exemple de la note)
    if commentaire:
        msg += f' ("{commentaire}")'

    return msg


def send_slack(webhook_url, message):
    """Envoie un message à Slack via webhook."""
    payload = {"text": message}
    response = requests.post(
        webhook_url,
        data=json.dumps(payload),
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    return response.status_code == 200


# --- MAIN ---
def main():
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

        webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        mock_mode = not webhook_url
        if mock_mode:
            print("MODE MOCK : pas de webhook Slack, messages en console")
        else:
            print("MODE SLACK : envoi réel des messages")


        # Récupérer les 20 dernières activités non notifiées
        # (limite à 20 pour ne pas spammer Slack en mode réel)
        cur.execute("""
            SELECT
                a.id,
                s.prenom,
                s.nom,
                a.type_sport,
                a.distance_m,
                EXTRACT(EPOCH FROM (a.date_fin - a.date_debut))::INTEGER AS duree_s,
                a.commentaire
            FROM raw.activites_sportives a
            JOIN raw.salaries s ON a.id_salarie = s.id_salarie
            WHERE a.slack_sent = FALSE
            ORDER BY a.date_debut DESC
            LIMIT 20
        """)
        activites = cur.fetchall()
        print(f"{len(activites)} activites a notifier")

        sent_ids = []
        for act_id, prenom, nom, sport, distance, duree, commentaire in activites:
            message = format_message(prenom, nom, sport, distance, duree, commentaire)

            if mock_mode:
                print(f"  [SLACK] {message}")
            else:
                success = send_slack(webhook_url, message)
                if success:
                    print(f"  [OK] Message envoyé pour {prenom} {nom}")
                else:
                    print(f"  [ERREUR] Échec envoi pour {prenom} {nom}")
                    continue

            sent_ids.append((act_id,))

        # Marquer les activités comme notifiées
        if sent_ids:
            from psycopg2 import extras
            extras.execute_values(
                cur,
                "UPDATE raw.activites_sportives SET slack_sent = TRUE WHERE id IN (VALUES %s)",
                sent_ids,
            )
            conn.commit()
            print(f"{len(sent_ids)} activites marquees comme notifiees")

        # Stats
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE slack_sent = TRUE) AS notifiees,
                COUNT(*) FILTER (WHERE slack_sent = FALSE) AS en_attente,
                COUNT(*) AS total
            FROM raw.activites_sportives
        """)
        row = cur.fetchone()
        print(f"\n  Notifiees   : {row[0]}")
        print(f"  En attente  : {row[1]}")
        print(f"  Total       : {row[2]}")

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


if __name__ == "__main__":
    main()
