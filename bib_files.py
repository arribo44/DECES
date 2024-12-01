
import os
import zipfile
import shutil
from pathlib import Path
from datetime import datetime

def create_directories(*paths):
    """
    Crée tous les répertoires spécifiés de manière récursive si ils n'existent pas déjà.

    Paramètre :
    - *paths : Chemins des répertoires à créer. Accepte un nombre variable d'arguments.
    """
    for path in paths:
        if not os.path.exists(path):
            try:
                # Création des répertoires récursivement si nécessaire
                os.makedirs(path, exist_ok=True)
                print(f"Répertoires créés avec succès : {path}")
            except Exception as e:
                print(f"Erreur lors de la création des répertoires {path}: {e}")

def delete_directories(*paths):
    """
    Delete tous les répertoires spécifiés de manière récursive .

    Paramètre :
    - *paths : Chemins des répertoires à deleter. Accepte un nombre variable d'arguments.
    """
    for path in paths:
        try:
            if os.path.exists(path):
                # Delete des répertoires récursivement
                shutil.rmtree(path)
                print(f"Répertoires deletés avec succès : {path}")
        except Exception as e:
            print(f"Erreur lors de la suppression des répertoires {path}: {e}")



def delete_files_in_dir(dir):
    """
    Deletes all files in the given directory. If any files are actually directories, it will delete those directories
    and all of their contents recursively.

    Parameters
    ----------
    dir : str
        The path to the directory containing the files to be deleted.

    Returns
    -------
    None

    """
    if not os.path.exists(dir):
        print(f"Le dossier '{dir}' n'existe pas.")
    else:
        for files in os.listdir(dir):
            path = os.path.join(dir, files)
            try:
                shutil.rmtree(path)
            except OSError:
                os.remove(path)

def get_creation_date_fic(sFilePath):
    #
    if  os.path.exists(sFilePath):
        # Obtenir le chemin du fichier
        file_path = Path(sFilePath)

        # Récupérer la date de création
        creation_time = file_path.stat().st_ctime  # st_ctime est la date de création

        # Convertir en format lisible
        return datetime.fromtimestamp(creation_time).strftime('%d/%m/%Y')
    else:
        print(f"\n### Le fichier '{sFilePath}' n'existe pas\n")
        exit(1)




# fonction zip de fichier
def zipCSV(file):
    """
    Zip the given file.

    Parameters
    ----------
    file : str
        The path to the file to be zipped.

    Returns
    -------
    None
    """
    zipobject=zipfile.ZipFile(file+'.zip','w')
    zipobject.write(file)
    zipobject.close()


def unzip_file(zip_path, extract_to=None):
    """
    Décompresse un fichier ZIP dans le répertoire spécifié ou, par défaut, dans le répertoire du fichier ZIP.

    :param zip_path: Chemin du fichier ZIP à décompresser.
    :param extract_to: Chemin de destination pour l'extraction. Si None, extrait dans le répertoire du fichier ZIP.
    """
    # Utiliser le répertoire du fichier ZIP comme destination par défaut
    if extract_to is None:
        extract_to = os.path.dirname(zip_path)

    # Vérification que le fichier est un fichier ZIP
    if zipfile.is_zipfile(zip_path):
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
            print(f"Extraction réussie dans le dossier : {extract_to}")
    else:
        print("Le fichier fourni n'est pas un fichier ZIP valide.")

