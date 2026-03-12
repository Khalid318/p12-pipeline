import pandas as pd
import psycopg2
import numpy as np

# Connexion à la base de données PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="sport_data",
    user="p12_user",
    password="p12_password"
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