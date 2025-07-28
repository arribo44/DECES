import pytest
from fonctions_utiles import generer_pays_viable  # Remplace 'ton_module' par le nom réel de ton fichier sans .py

@pytest.mark.parametrize("input_str,expected_output", [
    ("FRANCE METROPOLITAINE", "FRANCE"),
    ("(ESPAGNE)", "ESPAGNE"),
    ("REUNION", "LA REUNION"),
    ("CENTRAFRICAINE", "REPUBLIQUE CENTRAFICAINE"),
    ("VIET", "VIETNAM"),
    ("CALEDONIE", "NOUVELLE-CALEDONIE"),
    ("EN ITALIE", "ITALIE"),
    ("E ALLEMAGNE", "ALLEMAGNE"),
    ("    ITALIE ", "ITALIE"),
    ("EN BELGIQUE", "BELGIQUE"),
    ("AU CANADA", "CANADA"),
    ("(PORTUGAL)", "PORTUGAL"),
    ("'SUISSE", "SUISSE"),
    ("    MAROC ", "MAROC"),
])
def test_generer_pays_valide(input_str, expected_output):
    assert generer_pays_viable(input_str) == expected_output

#def test_generer_pays_invalide():
#    with pytest.raises(ValueError, match=r".*Format non pris en charge.*"):
#        generer_pays_viable("PLANETE MARS")
