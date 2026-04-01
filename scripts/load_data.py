"""
P12 - Sport Data Solution
Chargement des fichiers Excel dans raw.salaries et raw.sports_declares.
Ingestion pure : aucun calcul métier, aucune transformation.
"""

import os
import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv
import sys

load_dotenv()


def main():
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

        rh_path = os.getenv("RH_FILE", "data/donnees_RH.xlsx")
        sports_path = os.getenv("SPORTS_FILE", "data/donnees_sports.xlsx")

        # --- INGESTION 1 : SALARIES ---
        df_rh = pd.read_excel(rh_path)
        df_rh = df_rh.replace({np.nan: None})

        records_rh = list(df_rh[[
            "ID salarié", "Nom", "Prénom", "Date de naissance", "BU",
            "Date d'embauche", "Salaire brut", "Type de contrat",
            "Nombre de jours de CP", "Adresse du domicile", "Moyen de déplacement"
        ]].itertuples(index=False, name=None))

        query_rh = """
            INSERT INTO raw.salaries (
                id_salarie, nom, prenom, date_naissance, bu, date_embauche,
                salaire_brut, type_contrat, nb_jours_cp, adresse_domicile, moyen_deplacement
            )
            VALUES %s
            ON CONFLICT (id_salarie) DO UPDATE SET
                nom = EXCLUDED.nom,
                prenom = EXCLUDED.prenom,
                date_naissance = EXCLUDED.date_naissance,
                bu = EXCLUDED.bu,
                date_embauche = EXCLUDED.date_embauche,
                salaire_brut = EXCLUDED.salaire_brut,
                type_contrat = EXCLUDED.type_contrat,
                nb_jours_cp = EXCLUDED.nb_jours_cp,
                adresse_domicile = EXCLUDED.adresse_domicile,
                moyen_deplacement = EXCLUDED.moyen_deplacement
        """

        extras.execute_values(cur, query_rh, records_rh)
        conn.commit()
        print(f"{len(records_rh)} salaries charges dans raw.salaries")

        # --- INGESTION 2 : SPORTS DECLARES ---
        df_sports = pd.read_excel(sports_path)
        df_sports = df_sports[df_sports["ID salarié"].notna()]
        df_sports = df_sports.replace({np.nan: None})

        records_sports = list(df_sports[[
            "ID salarié", "Pratique d'un sport"
        ]].itertuples(index=False, name=None))

        query_sports = """
            INSERT INTO raw.sports_declares (id_salarie, sport)
            VALUES %s
            ON CONFLICT (id_salarie) DO UPDATE SET
                sport = EXCLUDED.sport
        """

        extras.execute_values(cur, query_sports, records_sports)
        conn.commit()
        print(f"{len(records_sports)} sports charges dans raw.sports_declares")

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