
import os
import yaml

class MissingKeyError(Exception):
    """Exception levée lorsqu'une clé obligatoire est absente."""
    def __init__(self, section, key):
        super().__init__(f"La clé '{key}' est absente dans la section '{section}'.")

def get_yaml_value(data, path, required=False, delimiter="."):
    """
    Récupère une valeur dans une structure YAML multi-niveaux.

    :param data: Dictionnaire chargé depuis un fichier YAML.
    :param path: Chemin vers la clé (liste ou chaîne délimitée).
    :param required: Booléen indiquant si la clé doit impérativement être présente.
    :param delimiter: Délimiteur pour les chemins de clés (par défaut ".").
    :return: La valeur associée à la clé ou None si absente (et non requise).
    :raises KeyError: Si une clé obligatoire est absente.
    """
    if isinstance(path, str):
        path = path.split(delimiter)

    current = data
    for key in path:
        if key not in current:
            if required:
                raise KeyError(f"Clé manquante : {'.'.join(path)}")
            return None
        current = current[key]
    return current

def set_yaml_value(data, path, value, delimiter="."):
    """
    Définit ou met à jour une valeur dans une structure YAML multi-niveaux.

    :param data: Dictionnaire chargé depuis un fichier YAML.
    :param path: Chemin vers la clé (liste ou chaîne délimitée).
    :param value: Valeur à associer à la clé.
    :param delimiter: Délimiteur pour les chemins de clés (par défaut ".").
    :return: Le dictionnaire mis à jour.
    """
    if isinstance(path, str):
        path = path.split(delimiter)

    current = data
    for key in path[:-1]:  # Parcourt tous les niveaux sauf le dernier
        if key not in current or not isinstance(current[key], dict):
            current[key] = {}
        current = current[key]
    current[path[-1]] = value
    return data


def save_yaml_to_file(file_path, yaml_data):
    """
    Sauvegarde une structure YAML en mémoire dans un fichier YAML.

    Paramètres :
    - file_path (str) : Chemin vers le fichier YAML.
    - yaml_data (dict) : Le contenu YAML à sauvegarder.
    """
    try:
        with open(file_path, 'w') as yaml_file:
            yaml.dump(yaml_data, yaml_file, default_flow_style=False)
    except Exception as e:
        print(f"Erreur lors de la sauvegarde du fichier YAML : {e}")


def load_yaml_from_file(file_path):
    """
    Charge une structure YAML en mémoire depuis un fichier YAML.

    Paramètres :
    - file_path (str) : Chemin vers le fichier YAML.
    """

    if os.path.exists(file_path):
        try:
            with open(file_path, "r") as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"Erreur lors de la sauvegarde du fichier YAML : {e}")
    else:
        print(f"\n### Le fichier '{file_path}' n'existe pas\n")
        exit(1)

