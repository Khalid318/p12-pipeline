"""
Tests unitaires pour google_maps.py
Vérifie l'extraction de code postal depuis différents formats d'adresse
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from google_maps import extract_code_postal

# Tests extraction code postal

def test_adresse_standard():
    """Adresse classique avec code postal au milieu"""
    assert extract_code_postal("128 Rue du Port, 34000 Montpellier") == "34000"

def test_adresse_avec_lattes():
    """Adresse de l'entreprise elle-même."""
    assert extract_code_postal("1362 Av. des Platanes, 34970 Lattes") == "34970"


def test_code_postal_en_debut():
    """Code postal au début de la chaîne."""
    assert extract_code_postal("34080 Montpellier") == "34080"


def test_adresse_nimes():
    """Adresse hors Hérault (Gard)."""
    assert extract_code_postal("15 Bd Victor Hugo, 30000 Nîmes") == "30000"

def test_adresse_vide():
    """Chaîne vide.Doit retourner None."""
    assert extract_code_postal("") is None

def test_adresse_none():
    """Valeur None. Doit retourner None."""
    assert extract_code_postal(None) is None

def test_adresse_sans_code_postal():
    """Adresse sans code postal. Doit retourner None."""
    assert extract_code_postal("128 Rue du Port, Montpellier") is None

def test_adresse_avec_numero_court():
    """Ne doit pas confondre un numéro de rue avec un code postal."""
    assert extract_code_postal("12 Rue de Paris, 75001 Paris") == "75001"
