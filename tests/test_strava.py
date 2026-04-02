"""
Tests unitaires pour generate_strava.py
Vérifie la génération d'activités sportives simulées.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from datetime import datetime
from generate_strava import generer_activite, determine_profil, PROFILS

# Tests generer_activite
def test_activite_course_a_distance():
    """Une activité course doit avoir une distance > 0"""
    act = generer_activite(1, "Course à pied", datetime(2025, 6, 15, 10, 0))
    # Retour : (id_salarie, date_debut, sport, distance_m, date_fin, commentaire)
    assert act[0] == 1                    # id_salarie
    assert act[2] == "Course à pied"      # type_sport
    assert act[3] is not None             # distance_m existe
    assert act[3] >= 3000                 # min du profil Course
    assert act[3] <= 20000               # max du profil Course

def test_activite_tennis_sans_distance():
    """Le tennis n'a pas de distance, seulement une durée"""
    act = generer_activite(5, "Tennis", datetime(2025, 6, 15, 14, 0))
    assert act[3] is None       # pas de distance
    assert act[4] > act[1]      # durée

def test_activite_date_fin_apres_debut():
    """La date de fin doit toujours être après la date de début."""
    for sport in ["Course à pied", "Vélo", "Yoga", "Natation"]:
        act = generer_activite(1, sport, datetime(2025, 7, 1, 8, 0))
        assert act[4] > act[1], f"date_fin <= date_debut pour {sport}"  

def test_activite_id_salarie_conserve():
    """L'id_salarie passé en entrée doit être celui en sortie."""
    act = generer_activite(42, "Marche", datetime(2025, 5, 10, 9, 0))
    assert act[0] == 42


# --- Tests determine_profil ---

def test_profil_sportif_declare():
    """Un salarié qui déclare un sport doit avoir ce sport dans sa liste."""
    sports, freq_min, freq_max = determine_profil("Natation", "véhicule thermique/éléctrique")
    assert "Natation" in sports
    assert freq_max > 0


def test_profil_transport_marche():
    """Un marcheur sans sport déclaré doit avoir Marche et Course."""
    sports, freq_min, freq_max = determine_profil(None, "Marche/running")
    assert "Marche" in sports
    assert "Course à pied" in sports


def test_profil_transport_velo():
    """Un cycliste sans sport déclaré doit avoir Vélo."""
    sports, freq_min, freq_max = determine_profil(None, "Vélo/Trottinette/Autres")
    assert "Vélo" in sports


def test_profil_aucun_sport_aucun_transport():
    """Quelqu'un en voiture sans sport → très peu d'activités."""
    sports, freq_min, freq_max = determine_profil(None, "véhicule thermique/éléctrique")
    assert freq_max <= 0.5  # quasi inactif
 