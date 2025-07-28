import pandas as pd
import os
from fonctions_utiles import copier_fichier

# copie du fichier deces.parquet vers insee_deces.parquet
sPath = "../DATAS/parquet/"
sFicIn = "deces.parquet"
sFicOut = "insee_deces.parquet"
copier_fichier(sPath + sFicIn, sPath + sFicOut)

# Lecture du fichier Parquet
df = pd.read_parquet(sPath + sFicIn)


# Vérification des valeurs exactes dans 'code_insee_deces' avant tout nettoyage
print("\nAperçu des premiers 'code_insee_deces' avant nettoyage:")
#print(df['code_insee_deces'].head(10))

# Vérification du type de la colonne
#print(f"\nType de 'code_insee_deces' avant nettoyage : {df['code_insee_deces'].dtype}")

# Conversion explicite en chaîne et nettoyage des espaces (y compris les espaces invisibles)
df['code_insee_deces'] = df['code_insee_deces'].astype(str).str.strip()

# Vérification après nettoyage
print("\nAperçu des premiers 'code_insee_deces' après nettoyage des espaces:")
print(df[df['code_insee_deces'] == ''].head(10))

# ✅ Liste des départements de la France métropolitaine
departements_metropole = [f"{i:02}" for i in range(1, 96)] + ['2A', '2B', '20']
print(f"\nListe des départements de la France métropolitaine : {departements_metropole}")

# 🔍 Fonction de validation des communes métropolitaines
def is_commune_metropolitaine(code_insee):
    if pd.isna(code_insee) or len(code_insee) != 5:
        return False
    code_dep = code_insee[:2]
    if code_dep in ['2A', '2B', '20'] :  # Cas spécial pour la Corse
        return True
    return code_dep in departements_metropole

# 🧪 Application de la validation des communes de naissance
df['naissance_valide'] = df['code_insee_naissance'].apply(is_commune_metropolitaine)
print(f"\nhead de 'naissance_valide' : {df['naissance_valide'].head(10)}")

# Vérification du nombre de naissances valides
nb_naissance_valide = df['naissance_valide'].sum()
print(f"\nNombre de lignes avec une commune de naissance valide (métropole) : {nb_naissance_valide}")

# 🧼 Création du masque pour 'code_insee_deces' vide ou composé uniquement d'espaces
masque_vide = df['code_insee_deces'].apply(lambda x: x.strip() == '')  # Vérification des vides après stripping
print(f"\nNombre de 'code_insee_deces' vides après nettoyage : {masque_vide.sum()}")

# Création du masque combiné
masque = masque_vide & df['naissance_valide']
print(f"\nNombre de lignes à corriger (décès vide ET naissance valide) : {masque.sum()}")

# 🛠 Remplacement des codes 'code_insee_deces' vides par 'code_insee_naissance'
df.loc[masque, 'code_insee_deces'] = df.loc[masque, 'code_insee_naissance']

# 🧽 Nettoyage de la colonne temporaire
df.drop(columns=['naissance_valide'], inplace=True)

# Vérification
print("\nAperçu des premiers 'code_insee_deces' après copie de 'code_insee_naissance' ")
print(df[df['code_insee_deces'] == ''].head(10))

# (Optionnel) Sauvegarde du DataFrame corrigé
df.to_parquet(sPath + sFicIn, index=False)
