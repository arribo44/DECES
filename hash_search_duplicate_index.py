from tqdm import tqdm
import pandas as pd
import hashlib
import unicodedata

tqdm.pandas()  # Active la barre de progression pour pandas

def normalize(text):
    if pd.isna(text):
        return ''
    text = str(text).strip().lower()
    # Supprimer les accents
    return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')

def compute_hash_id(row, length=20):
    components = [
        normalize(row['nom']),
        normalize(row['prenoms']),
        row['sexe'].strip().upper() if pd.notna(row['sexe']) else '',
        str(row['date_naissance']),
        str(row['code_insee_naissance']),
        str(row['commune_naissance']),
        str(row['pays_naissance']),
        str(row['date_deces']),
        str(row['code_insee_deces']),

    ]
    base_str = '|'.join(components)
    hash_val = hashlib.sha256(base_str.encode('utf-8')).hexdigest()
    return hash_val[:length]


def detect_hash_duplicates(df, id_col='ID_HASH'):
    # Étape 1 : Trouver les valeurs dupliquées
    duplicated_ids = df[id_col][df[id_col].duplicated(keep=False)]

    # Étape 2 : Filtrer les lignes qui ont des IDs dupliqués
    duplicates_df = df[df[id_col].isin(duplicated_ids)]

    # Étape 3 : Nombre d'occurrences par ID
    counts = duplicates_df.groupby(id_col).size().reset_index(name='nb_occurrences')

    # Étape 4 : Joindre les comptes aux lignes
    result = duplicates_df.merge(counts, on=id_col)

    return result.sort_values(by='nb_occurrences', ascending=False)


# Chargement du fichier parquet
df = pd.read_parquet('../DATAS/parquet/deces_origine_202503.parquet')

# Création de la colonne ID (hash sur 12 caractères)
#df['ID_HASH'] = df.apply(compute_hash_id, axis=1)

# Application avec barre de progression
df['ID_HASH'] = df.progress_apply(compute_hash_id, axis=1)

# Sauvegarde
df.to_parquet('../DATAS/parquet/deces_hash.parquet', index=False)

# Appel de la fonction sur ton DataFrame
# dupes = detect_hash_duplicates(df, id_col='ID')

# Affichage du nombre de collisions
# print(f"Nombre de hash dupliqués : {dupes['ID_HASH'].nunique()}")
# print(f"Nombre total de lignes concernées : {len(dupes)}")

# Aperçu des doublons
# print(dupes.head(10))
