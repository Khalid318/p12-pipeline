"""
Tests unitaires pour slack_notify.py
Vérifie le formatage des messages de félicitations slack.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from slack_notify import format_message

# Tests format_message
def test_message_course_avec_distance():
    """Course à pied : le message doit contenir le nom et la distance en km."""
    msg = format_message("Juliette", "Mendes", "Course à pied", 10800, 2760, None)
    assert "Juliette" in msg
    assert "Mendes" in msg
    assert "10.8" in msg  # 10800m → 10.8 km

def test_message_natation_distance_en_metres():
    """Natation: la distance doit rester en mètres, pas convertie en km."""
    msg = format_message("Paul", "Dupont", "Natation", 1500, 1800, None)
    assert "1500" in msg # pas 1.5 km

def test_message_sport_sans_distance():
    """Tennis : pas de distance, le message doit contenir le nom et le sport."""
    msg = format_message("Marie", "Durand", "Tennis", None, 3600, None)
    assert "Marie" in msg
    assert "Tennis" in msg

def test_message_avec_commentaire():
    """Si commentaire présent, il doit apparaître dans le message."""
    msg = format_message("Ali", "Zidane", "Vélo", 25000, 3600, "Sortie en groupe")
    assert "Sortie en groupe" in msg        

def test_message_sans_commentaire():
    """Sans commentaire, pas de parenthèses vides."""
    msg = format_message("Sara", "Martin", "Yoga", None, 2700, None)
    assert "()" not in msg
    assert '("")' not in msg    

def test_message_sport_inconnu_utilise_default():
    """Un sport pas dans TEMPLATES doit quand même générer un message."""
    msg = format_message("Luc", "Bernard", "Pétanque", None, 3600, None)
    assert "Luc" in msg
    assert "Bernard" in msg
