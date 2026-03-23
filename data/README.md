# Données sources

Ce dossier contient les fichiers Excel nécessaires au pipeline.
Ils ne sont pas versionnés sur GitHub (données RH sensibles — RGPD).

## Fichiers attendus

### donnees_RH.xlsx

161 lignes, 11 colonnes :

| Colonne | Type | Exemple |
|---------|------|---------|
| ID salarié | Entier | 59019 |
| Nom | Texte | Colin |
| Prénom | Texte | Audrey |
| Date de naissance | Date | 1990-07-06 |
| BU | Texte | Marketing, Finance, Support, Ventes, R&D |
| Date d'embauche | Date | 2018-03-15 |
| Salaire brut | Décimal | 30940.00 |
| Type de contrat | Texte | CDI, CDD |
| Nombre de jours de CP | Entier | 25 |
| Adresse du domicile | Texte | 128 Rue du Port, 34000 Frontignan |
| Moyen de déplacement | Texte | Vélo/Trottinette/Autres, Marche/running, véhicule thermique/électrique, Transports en commun |

### donnees_sports.xlsx

999 lignes (161 utiles, 838 avec ID vide), 2 colonnes :

| Colonne | Type | Exemple |
|---------|------|---------|
| ID salarié | Entier | 59019 |
| Pratique d'un sport | Texte | Runing, Tennis, Football, Natation... (ou vide) |

## Pour reproduire le pipeline

Si vous n'avez pas les fichiers originaux, créez deux fichiers Excel avec la structure ci-dessus et quelques lignes de test. Le pipeline fonctionnera avec n'importe quel volume de données.
