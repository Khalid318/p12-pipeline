import pandas as pd
import psycopg2
import numpy as np
import os
from dotenv import load_dotenv


# Connexion à la base de données PostgreSQL

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)
cur = conn.cursor()

print("Connexion réussie !")

# Chargement du fichier RH
df_rh = pd.read_excel("data/donnees_RH.xlsx")
print(df_rh.columns)

row = df_rh.iloc[0]
print(row)

# Insertion des données RH dans la table salaries
for index, row in df_rh.iterrows():
    cur.execute("""
        INSERT INTO salaries (
            id_salarie,
            nom,
            prenom,
            date_naissance,
            bu,
            date_embauche,
            salaire_brut,
            type_contrat,
            nb_jours_cp,
            adresse_domicile,
            moyen_deplacement
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
    """, (
        row["ID salarié"],
        row["Nom"],
        row["Prénom"],
        row["Date de naissance"],
        row["BU"],
        row["Date d'embauche"],
        row["Salaire brut"],
        row["Type de contrat"],
        row["Nombre de jours de CP"],
        row["Adresse du domicile"],
        row["Moyen de déplacement"]
    ))

conn.commit()


# Chargement et insertion des données sports
df_sports = pd.read_excel("data/donnees_sports.xlsx")
print(df_sports.columns)
row = df_sports.iloc[0]
print(row)
print(df_sports.isna().sum())
df_sports = df_sports.replace({np.nan: None})
df_sports = df_sports[df_sports["ID salarié"].notna()]

cur.execute("TRUNCATE TABLE sports_declares RESTART IDENTITY;")
# Insertion des sports déclarés dans la table sports_declares
for index, row in df_sports.iterrows():
    cur.execute("""
        INSERT INTO sports_declares (
            id_salarie,
            sport
        )
        VALUES (%s, %s)
    """, (
        row["ID salarié"],
        row["Pratique d'un sport"]
    ))

conn.commit()


# Fermeture de la connexion
cur.close()
conn.close()