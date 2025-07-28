
from flashtext import KeywordProcessor
import pickle
import json

prenoms = ["Jean", "Marie", "Michel", "Sophie", "Mohamed", "Julie", "Ali", "Fatima", "Said"]

kp = KeywordProcessor(case_sensitive=False)
for prenom in prenoms:
    kp.add_keyword(prenom)

textes = ["Dupont Marie est née à Paris", "SAID TOTO", "FATIMA ASSANI est née à Paris", "Robert ASALINI est née à Paris"]
for texte in textes:
    resultats = kp.extract_keywords(texte)
    print(resultats)


modifications_diverses = {
    '022b8b0324fa5349c735': {'prenoms': 'JOSEPH'},
    '68eee1980427e3845570': {'prenoms': 'LOUIS', 'nom': 'FEREOLE'},
    '1496dc87a263323513e2': {'prenoms': 'SAID', 'nom': 'TOTO'},
    '85831ab02e51ea028298': {'prenoms': 'SERGE,FRANCOIS,MAX'},
    '62810fbd3e4e9705eec2': {'prenoms': 'DESROCHES,FRANCE,FERNANDE'},
    'caca556b70dab229516d': {'prenoms': 'MARIE,BLANCHE,AUGUSTINE'},
    '178e6790c839387389ba': {'prenoms': 'MARIA','nom': 'CANDIDA'},
    '52b56073991cd6ed73b3': {'prenoms': 'ANASTACIA', 'nom': 'CANDIDA'},
}

save_modifications_diverses = {
    '376cfd984a369a1d082e': {'code_insee_naissance': '99101'},
    'c40e420ed23737e2ebcc': {'commune_naissance': 'AARHUS'},
    '57898c64d8c75d850fc7': {'pays_naissance': 'BANGLADESH'},
}

# Sauvegarde en Pickle
with open("modifications_diverses.pkl", "wb") as f_pickle:
    pickle.dump(modifications_diverses, f_pickle)

# Sauvegarde en JSON
with open("modifications_diverses.json", "w", encoding="utf-8") as f_json:
    json.dump(modifications_diverses, f_json, ensure_ascii=False, indent=2)

print("Fichiers enregistrés : modifications_diverses.pkl et modifications_diverses.json")
