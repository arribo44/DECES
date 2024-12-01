

import random
from datetime import datetime


def dates_equals(date1_str, date2_str, date_format='%d/%m/%Y'):
    """
    Compare deux dates sous forme de chaînes de caractères.

    Paramètres :
    - date1_str (str) : La première date sous forme de chaîne.
    - date2_str (str) : La seconde date sous forme de chaîne.
    - date_format (str) : Le format des dates (par défaut '%d/%m/%Y').

    Retourne :
    - bool : True si les dates sont identiques, False sinon.

    Gère :
    - ValueError si le format des dates est incorrect.
    """
    try:
        # Convertir les chaînes en objets datetime
        date1 = datetime.strptime(date1_str, date_format)
        date2 = datetime.strptime(date2_str, date_format)
        # Comparer les dates
        return date1 == date2
    except ValueError as e:
        print(f"Erreur : l'une des dates n'a pas le bon format. {e}")
        return False


def date_superieur(date1_str, date2_str, date_format='%d/%m/%Y'):
    """
    Compare deux dates sous forme de chaînes de caractères.

    Paramètres :
    - date1_str (str) : La première date sous forme de chaîne.
    - date2_str (str) : La seconde date sous forme de chaîne.
    - date_format (str) : Le format des dates (par défaut '%d/%m/%Y').

    Retourne :
    - bool : True si la premiere date est superieure, False sinon.

    Gère :
    - ValueError si le format des dates est incorrect.
    """
    try:
        # Convertir les chaînes en objets datetime
        date1 = datetime.strptime(date1_str, date_format)
        date2 = datetime.strptime(date2_str, date_format)
        # Comparer les dates
        return date1 > date2
    except ValueError as e:
        print(f"Erreur : l'une des dates n'a pas le bon format. {e}")
        return False



# Liste des mois qui peuvent avoir 31 jours
mois_avec_31_jours = ['01', '03', '05', '07', '08', '10', '12']

# Fonction pour vérifier si une année est bissextile
def est_bissextile(annee):
    # Une année est bissextile si elle est divisible par 4, mais pas divisible par 100 sauf si divisible par 400
    return (annee % 4 == 0 and (annee % 100 != 0 or annee % 400 == 0))



def correction_date(jour, mois, annee):

    if annee == "0000":
        # Génère une annee aléatoire entre 1940 et 1970
        annee_debut = random.randint(1940, 1950)
        annee_fin = random.randint(1960, 1970)
        annee = random.randint(annee_debut, annee_fin)

    # Correction du mois si la valeur est "00"
    # print(f"Fonction correction_date : params : {jour} {mois} {annee}")
    if mois == "00":
        # Génère un mois aléatoire entre 1 et 12
        mois = random.randint(1, 12)

        #print(f"1 / Le mois est : {mois}")
        # Si le mois généré est février (2), ajoute un nombre aléatoire de 1 à 10 pour modifier le mois
        #if mois == 2:
        #    mois += random.randint(1, 10)

        # Formate iDBMois pour avoir deux chiffres
        mois = f"{mois:02}"

    # print(f"1 / Le mois est : {mois}")
    if int(mois) > 12:
        mois="12"
    #     print("##### ERRERUR GENERATION MOIS #####")

    #print(f"2 / Le mois est : {mois}")
    if int(jour) == 0 or int(jour) > 31:
        #print("DB Correction JOUR " + jour)
        jour = random.randint(1, 31)
        jour = f"{jour:02}"

    # print(f"1 / Le jour est : {jour}")
    # if int(jour) > 31:
    #     print("##### 1 / ERRERUR GENERATION JOUR #####")

    # Vérification de la condition
    if jour == "31" and jour not in mois_avec_31_jours:
        jour = "30"

    # if int(jour) > 31:
    #     print("##### 2 / ERRERUR GENERATION JOUR #####")

    # Vérification si le jour est supérieur à 29 et que c'est février
    if int(jour) >= 29 and mois == '02':
        # print(f"le jour est supérieur à 29 et que c'est février :  {iDBJour} {iDBMois} {iDBAnnee}")
        if est_bissextile(int(annee)):
            jour = "29"  # Si l'année est bissextile, le jour devient 29
        else:
            jour = "28"  # Si l'année n'est pas bissextile, le jour devient 28

    # if int(jour) > 31:
    #     print("##### 3 / ERRERUR GENERATION JOUR #####")
    #
    return jour, mois, annee