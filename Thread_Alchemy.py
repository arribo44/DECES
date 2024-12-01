#
import traceback
import argparse
import calendar
import csv
import glob
import multiprocessing
import threading
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing.dummy import Pool as ThreadPool
import concurrent.futures
import os, time, re
import random
import shutil
from pathlib import Path
import subprocess
from collections import Counter
from datetime import date, datetime

from tqdm import tqdm
#
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
#

#
import yaml
#
from pathlib import Path

from rapidfuzz import fuzz, process

from dict_transco import corrections_prenoms, corrections_noms, noms_prenoms_dict, REGIONS, DPTS

from sqlalchemy_classes import Personne, Prenoms, FRANCE_LOC, COUNTRIES_LOC, UNKNOWN_LOC, retry_commit
from sqlalchemy.exc import OperationalError, SQLAlchemyError, IntegrityError

from fonctions_utiles import correction_date,  dates_equals,    date_superieur
from bib_yaml import load_yaml_from_file, save_yaml_to_file,  get_yaml_value, MissingKeyError, set_yaml_value
from bib_files import create_directories, delete_directories, delete_files_in_dir, get_creation_date_fic

#
global dict_coord_france, dict_coord_foreign, dict_coord_unknown
dict_coord_unknown = {}

# Options du script
# opts = [opt for opt in sys.argv[1:] if opt.startswith("-")]
# args = [arg for arg in sys.argv[1:] if not arg.startswith("-")]

PERFORM = {
    "PREPARE_DIR": "Prepare All the directory and files for a year",
    "PREPARE_DATAS": "Prepare All the contents in files for a year",
    "LOAD": "load all the contents in DB form files for a year",
    "PERSON": "Check if the person is in the DB",
    "CITYDB": "Only the Birth City",
    "CITYDD": "Only the Death City",
    "DIST": "Only the records without calculated distance",
    "SQLDB": "Top SQL of the DB City unknown"
}


# list des communes avec un pb de coordonnees geo
communes_pb = []

# Lecture du fichier Yaml
global config_yaml
sFicYaml="properties.yaml"
config_yaml=load_yaml_from_file(sFicYaml)

# parametre pour le split de fichier
try:
    splitLimite=get_yaml_value(config_yaml, "main.spliting.splitlimite", required=True)
    ChunkSize=get_yaml_value(config_yaml, "main.spliting.chunksize", required=True)
except MissingKeyError as e:
    print(e)
    exit(1)


# TimeOur SGBD SQLite
TimeOutSqlLite = 60

#
from sqlalchemy import create_engine, or_, update, text, func, delete
from sqlalchemy.orm import sessionmaker, scoped_session
#from sqlalchemy.exc import SQLAlchemyError

# Création du moteur de base de données SQLite
try:
    db_path=get_yaml_value(config_yaml, "main.sgbd.db_path", required=True)
    db_name=get_yaml_value(config_yaml, "main.sgbd.db_name", required=True)
    TimeOutSqlLite=get_yaml_value(config_yaml, "main.sgbd.timeoutsqlite", required=True)
except MissingKeyError as e:
    print(e)
    exit(1)

sDBPath = os.path.join(os.path.abspath(os.path.join(db_path, db_name)))

# Créer l'engine SQLite avec des arguments de connexion spécifiques
if os.path.exists(sDBPath):
    engine = create_engine(
        'sqlite:///' + sDBPath,
        connect_args={'timeout': TimeOutSqlLite, 'check_same_thread': False}
    )
else:
    print(f"\n### Le fichier '{sDBPath}' n'existe pas\n")
    exit(1)

# Activer le mode Write-Ahead Logging (WAL)
with engine.connect() as connection:
    wal_mode = connection.execute(text("PRAGMA journal_mode=WAL")).scalar()
    if wal_mode == "wal":
        print("Mode WAL activé avec succès.")
    else:
        print("Échec de l'activation du mode WAL.")


# Création de la session
Sessions = sessionmaker(bind=engine)

Sess_deces = Sessions()

# importing geopy library
from geopy.geocoders import Nominatim
from geopy.distance import geodesic

# calling the Nominatim tool
loc = Nominatim(user_agent="GetLoc")

# Variables
sEnTetePersonnes = ['nom', 'prenom', 'sexe', 'age', 'long_nom',
                    'nbre_prenoms', 'db_an', 'db_mois', 'db_jour', 'db_lib_jour',
                    'db_week', 'db_code_commune', 'db_lib_commune', 'db_code_dpt', 'db_pays',
                    'db_continent', 'dd_an', 'dd_mois', 'dd_jour', 'dd_lib_jour',
                    'dd_week', 'dd_code_commune', 'dd_lib_commune', 'dd_code_dpt', 'dd_pays',
                    'dd_continent', 'iso_date', 'iso_dpt', 'iso_commune', 'iso_pays',
                    'distance']
# iStringPersonnes=(0,1,6,8,9,10,12,13,14,15,16,17,18,19,20,22,23,24,25,25,26)

sEnTetePrenoms = ['evt', 'level', 'sexe', 'annee', 'mois', 'jour', 'code_dpt', 'prenom', 'fichier']

days = ['Lundi', 'Mardi', 'Mercredi', 'Jeudi', 'Vendredi', 'Samedi', 'Dimanche']

dict_StatPrenoms = {}

# Liste des mois qui peuvent avoir 31 jours
mois_avec_31_jours = ['01', '03', '05', '07', '08', '10', '12']


# Fonction de correction des prenoms
def correct_surnames(surnames):

    if surnames is None:
        return None

    for old, new in corrections_prenoms.items():
        surnames = surnames.replace(old, new)
    return surnames


def correct_names(names):
    #print("ici")
    if names is None:
        return None

    for old, new in corrections_noms.items():
        #print(f"{old} -> {new}")
        if names == old :
            #print(f"Remplacement identique de '{names}' == '{old}' par '{new}'")
            names = new
        elif re.search(old, names):
            #print(f"Remplacement regexp de '{names}' proche de'{old}' par '{new}'")
            #names = names.replace(old, new)
            names=re.sub(old, new, names)

    return names


# Fonctionn pour corriger et intervertir nom et prenom cf HOUEL DE LA RUFFINIERE
def corriger_nom_prenom(dataset, corrections):

    for prenom, nom_faux in corrections.items():
        # Identifier les indices où la condition est vraie
        indices = dataset[(dataset["nom"] == prenom) & (dataset["prenoms"] == nom_faux[0])].index

        # Appliquer les modifications en utilisant les indices
        for idx in indices:
            # Remplacer la virgule par un espace dans la colonne "nom"
            dataset.at[idx, "nom"] = nom_faux[0].replace(",", " ")

            # Remplacer l'espace par une virgule dans la colonne "prenom"
            dataset.at[idx, "prenoms"] = prenom.replace(" ", ",")

        # Afficher les lignes corrigées
        #print(f"Lignes corrigées pour {prenom} et {nom_faux} :\n", dataset.loc[indices])

    return dataset


# Fonction pour comparer les noms
#def trouver_similarites(dataframe, colonne, seuil=90):
def detect_similar_names(df, colonne, threshold=85):
    # Diviser le dataset par longueur de nom
    very_short_names = df[df[colonne].str.len() < 5]
    short_names = df[(df[colonne].str.len() >= 4) & (df[colonne].str.len() <= 10)]
    medium_names = df[(df[colonne].str.len() >= 8) & (df[colonne].str.len() <= 15)]
    long_names = df[(df[colonne].str.len() >= 13) & (df[colonne].str.len() <= 20)]
    very_long_names = df[df[colonne].str.len() > 18]

    results = []

    print(f"## Comparaison des noms ")
    print(f"## Nombre de noms tres courts : {very_short_names.shape[0]}")
    print(f"## Nombre de noms court : {short_names.shape[0]}")
    print(f"## Nombre de noms moyen : {medium_names.shape[0]}")
    print(f"## Nombre de noms long : {long_names.shape[0]}")
    print(f"## Nombre de noms longs : {very_long_names.shape[0]}")

    def find_matches(df_group):
        group_results = []
        names = df_group['nom'].unique()
        for name in names:
            matches = process.extract(
                name, names, scorer=fuzz.ratio, limit=5
            )
            for match, score, _ in matches:
                if score >= threshold and name != match:
                    group_results.append(f"'{name}' est proche de '{match}' avec un score de {score}")
        return group_results

    # # Exécuter find_matches en parallèle sur chaque sous-groupe
    with ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(find_matches, very_short_names),
            executor.submit(find_matches, short_names),
            executor.submit(find_matches, medium_names),
            executor.submit(find_matches, long_names),
            executor.submit(find_matches, very_long_names)
        ]
        for future in futures:
            results.extend(future.result())

    # Exécuter find_matches en parallèle sur chaque sous-groupe
    # with ThreadPoolExecutor() as executor:
    #     futures = [
    #         executor.submit(find_matches, very_long_names)
    #     ]
    #     for future in futures:
    #         results.extend(future.result())

    return results


#
def get_personne(sess_pers, birth_year, birth_month, birth_day, last_name, first_name, sexe):
    print("get_personne")
    try:
        # Query the database using the provided criteria
        record = sess_pers.query(Personne).filter_by(
            db_an=birth_year,
            db_mois=birth_month,
            db_jour=birth_day,
            nom=last_name,
            prenom=first_name,
            sexe=sexe
        ).first()

        # Get the first matching record
        # record = query
        # print(f"Nbre record : {record}")
        return record

    except SQLAlchemyError as e:
        print(f"An error occurred while retrieving the record: {e}")
        return None


def charger_insee_geo(session):
    """
    Charge les colonnes 'Code_INSEE' et 'geo_point_2d' de la table FRANCE_LOC dans un dictionnaire.
    Le dictionnaire inclut 'Code_INSEE' et 'Code_Old_INSEE' comme clés, chacune pointant vers 'geo_point_2d' comme valeur.

    :param session: La session SQLAlchemy utilisée pour la requête.
    :return: Un dictionnaire avec 'Code_INSEE' et 'Code_Old_INSEE' comme clés et 'geo_point_2d' comme valeur.
    """
    # Exécute la requête pour récupérer les colonnes 'Code_INSEE', 'Code_Old_INSEE' et 'geo_point_2d'
    result = session.query(FRANCE_LOC.Code_INSEE, FRANCE_LOC.Code_Old_INSEE, FRANCE_LOC.Commune, FRANCE_LOC.geo_point_2d).all()

    # Construit le dictionnaire
    insee_geo_dict = {}

    for row in result:
        # Ajoute 'Code_INSEE' comme clé si elle existe
        insee_geo_dict[row.Code_INSEE] = {
            "Commune": row.Commune,
            "geo_point_2d": row.geo_point_2d
            }

        # Ajoute 'Code_Old_INSEE' comme clé si elle existe
        if row.Code_Old_INSEE:
            insee_geo_dict[row.Code_Old_INSEE] = {
                "Commune": row.Commune,
                "geo_point_2d": row.geo_point_2d
            }

    return insee_geo_dict


def charger_countries_geo(session) -> dict:
    """
    Charge la table COUNTRIES_LOC dans un dictionnaire.
    La clé du dictionnaire est la colonne 'Code_INSEE', et les valeurs sont les autres colonnes.

    Args:
        session (Session): La session SQLAlchemy.

    Returns:
        dict: Dictionnaire avec 'Code_INSEE' comme clé et un dictionnaire des autres colonnes comme valeur.
    """
    countries_dict = {}

    # Requête pour récupérer toutes les lignes de la table
    results = session.query(COUNTRIES_LOC).all()

    # Parcours des résultats et remplissage du dictionnaire
    for row in results:
        countries_dict[row.Code_INSEE] = {
            "Country": row.Country,
            "Capital": row.Capital,
            "geo_point_2d": row.geo_point_2d,
            "Continent": row.Continent,
        }

    return countries_dict

#################################
## Fonctions sur la localisation
#################################

# Test if exist place in table 'localisation'
def check_localisation(sess_loc, idCommune, sNomCommune, sPays):
    print("check_localisation")
    try:
        if sPays == 'FRANCE':
            # print(f"check_localisation {idCommune}, {NomCommune}, {Pays}"
            record = sess_loc.query(Localisations).filter_by(idCommune=idCommune, Pays='FRANCE').first()
            return record is not None
        else:
            record = sess_loc.query(Localisations).filter_by(NomCommune=sNomCommune, Pays=sPays).first()
            return record is not None
    except SQLAlchemyError as e:
        print(f"An error occurred while checking the record: {e}")
        return False


# Test if exist place_id already
def check_placeid(sessionPlace, place_id):
    try:
        # print(f"check_localisation {place_id}")
        record = sessionPlace.query(Localisations).filter_by(place_id=place_id).first()
        # sessionPlace.close()
        if record is not None:
            return True
        else:
            return False
    except SQLAlchemyError as e:
        print(f"An error occurred while checking the record: {e}")
        return False


#
def get_localisation_info(city_name):
    # Créer une instance de géocodeur Nominatim
    geolocator = Nominatim(user_agent="my_app")

    print(f"\tget_localisation_info : {city_name}")
    try:
        # Rechercher les informations sur la ville
        location = geolocator.geocode(city_name)

        if location:
            print("\tGEOLOCATOR OK :", location.raw['display_name'])
            # print("Latitude :", location.latitude)
            # print("Longitude :", location.longitude)
            # print("Code postal :", location.raw['address'].get('postcode', 'N/A'))
            # print("Pays :", location.raw['address'].get('country', 'N/A'))
            # Vous pouvez accéder à plus d'informations dans le dictionnaire 'raw'
            return location.raw

        else:
            print("\#### Aucune information trouvée pour la ville spécifiée. ####")
            return None

    except Exception as e:
        print(f"\t##### Une erreur s'est produite : {e} ####")
        return None


# Create record
def create_localisation(session_create, place_id, display_name, idCommune, NomCommune, Nom2Commune, Pays, Lat, Long):
    print("\t\n**** create_localisation ****\n")
    try:
        new_record = Localisations(
            place_id=place_id,
            display_name=display_name,
            idCommune=idCommune,
            NomCommune=NomCommune,
            Nom2Commune=Nom2Commune,
            Pays=Pays,
            Lat=Lat,
            Long=Long
        )
        session_create.add(new_record)
        session_create.commit()
        # session_create.close()
        return new_record.place_id
    except SQLAlchemyError as e:
        print(f"\t\n #### An error occurred while creating the record: {e} ####")
        return None


# Get location and distance
# Recherche des villes de deces sans nom
def issue_dd_commune(sess_dist):
    try:
        # Query to retrieve specific columns
        columns_to_select = [Personne.dd_code_commune]
        records = sess_dist.query(*columns_to_select).filter(Personne.dd_lib_commune == '_').distinct().all()
        return records

    except SQLAlchemyError as e:
        print("An error occurred:", str(e))
        # Handle the error as needed
        return []


#
def issue_distance(sess_dist, year):
    try:
        # Query to retrieve specific columns
        columns_to_select = [Personne.db_code_commune, Personne.dd_code_commune]
        records = sess_dist.query(*columns_to_select).filter(
            Personne.dd_an == year,
            Personne.distance == 99999
        ).distinct().all()

        return records

    except SQLAlchemyError as e:
        print("An error occurred:", str(e))
        # Handle the error as needed
        return []


def get_lat_long_by_place_id(sessionTmp, idCommune):
    # print(f"get_lat_long_by_place_id : {idCommune}")
    global communes_pb

    try:
        if idCommune[0:2] != "99":
            # sessionTmp = Sess_deces()
            #pattern = '%' + idCommune + '%'
            record = sessionTmp.query(FRANCE_LOC).filter(
                or_(FRANCE_LOC.Code_INSEE == idCommune, FRANCE_LOC.Code_Old_INSEE == idCommune))

            # Afficher la requête SQL
            print(str(record.statement.compile(compile_kwargs={"literal_binds": True})))

            # Exécuter la requête
            record = record.one()

        else:
            record = sessionTmp.query(COUNTRIES_LOC).filter(COUNTRIES_LOC.Code_INSEE == idCommune).one()
        # print(f"1 - Get Loc : {record}")
        # print(f"2 - Get Loc : {type(record)}")
        # print(f"3 - Get Loc : {record.geo_point_2d}")
        # sessionTmp.close()
        lat, long = record.geo_point_2d.split(',')
        #print(f"\n:{lat.strip()}:")
        #print(f":{long.strip()}:")
        return lat, long
    except SQLAlchemyError as e:
        # print(f"\t\n#### KO pour retrouver lat,long de : {idCommune} ####")
        communes_pb.append(idCommune)
        return None


def Calc_Distance(iDBCoord, iDDCoord, sNoms):
    #
    # print("Fonction Calc_Distance")
    #iCoordDB = get_lat_long_by_place_id(Sess_deces, iComDB)
    #iCoordDD = get_lat_long_by_place_id(Sess_deces, iComDD)

    #iCoordDB = dict_coord_france.get(iComDB, None)
    #iCoordDD = dict_coord_france.get(iComDD, None)

    if iDBCoord is not None and iDDCoord is not None:
        try:
            #print("Calc_Distance : OK")
            iDist = geodesic(iDBCoord, iDDCoord).km
            #print(f"Distance : {iDist}")
            return round(iDist)

        except Exception as e:
            print(f"Calc_Distance pour {sNoms} : KO : {e}")
            return 4444.4444
    else:
        #print("Calc_Distance : KO")
        return 4444.4444


def update_distance(sessionTmp, db_code_commune, dd_code_commune, new_distance):
    try:

        # Update the 'distance' column based on 'db_code_commune' and 'dd_code_commune'
        stmt = update(Personne).where(
            Personne.db_code_commune == db_code_commune,
            Personne.dd_code_commune == dd_code_commune
        ).values(distance=new_distance)

        # Execute the update statement
        sessionTmp.execute(stmt)
        sessionTmp.commit()

        # print("Distance updated successfully.")

    except SQLAlchemyError as e:
        print("An error occurred:", str(e))
        # Handle the error as needed


def delete_dans_unknown_loc(annee):

    #print(f"SUPPRESSIONNNNNNNNNN")
    # Créer une session propre à chaque processus
    Session_insert = Sessions()
    Session_insert.execute(text("PRAGMA journal_mode=WAL;"))
    Session_insert.commit()

    try:
        Session_insert.execute(delete(UNKNOWN_LOC).where(UNKNOWN_LOC.Fichier == str(annee)))
        Session_insert.commit()
    except Exception as e:
        print("Pb de traitement de suppression dans unknown loc, l'erreur est la suivante : {e}")
        traceback.print_exc()

def inserer_dans_unknown_loc(dict_inserer):

    #print("Dans la fonction inserer_dans_unknown_loc")

    # Créer une session propre à chaque processus
    Session_insert = Sessions()
    Session_insert.execute(text("PRAGMA journal_mode=WAL;"))
    Session_insert.commit()

    # Insère les nouvelles données dans UNKNOWN_LOC
    for code_insee, data in dict_inserer.items():
        #print(code_insee)
        try:

            nouveau_record = UNKNOWN_LOC(
                Code_INSEE=code_insee,
                Commune=data['Commune'],
                Pays=data['Pays'],
                Fichier=str(data['Fichier'])  # Utilise 'Annee' pour remplir 'Fichier'
            )
            #print(nouveau_record)
            Session_insert.add(nouveau_record)

        except Exception as e:
            print(f"L'erreur est la suivante : {e}")
            traceback.print_exc()

    # Commmit les nouvelles données
    try:
        Session_insert.commit()
        #print("Données unknown_loc insérées avec succès après suppression.")
    except Exception as e:
        Session_insert.rollback()
        print(f"Erreur lors de l'insertion unknown_loc: {e}")

# Write a set/list to file
def write_set_to_file(filename, input, sessionTmp):
    # Convert the set to a string
    # set_string = str(input_set)

    # Write the string to a file
    with open(filename, 'w') as file:
        for commune, count in input.items():
            file.write(f"{commune}: {count}\n")
            db_records = sessionTmp.query(Personne.db_lib_commune).filter(
                Personne.db_code_commune == commune).distinct().all()
            file.write(f"db_lib_commune: {db_records}\n")
            # dd_records = sess_dist.query(Personne.dd_code_commune).filter(Personne.dd_lib_commune == commune).distinct().all()
            # file.write(f"db_lib_commune: {records}\n")


# Compteur des prenoms
def fCptCles(arrCles):
    for sLaCle in arrCles:
        if sLaCle in dict_StatPrenoms:
            dict_StatPrenoms[sLaCle] = dict_StatPrenoms[sLaCle] + 1
        else:
            dict_StatPrenoms[sLaCle] = 1


# Calcul signe zodiaque
# def zodiac_sign(day, month):
#     # checks month and date within the valid range
#     # of a specified zodiac
#     # global astro_sign
#     if month == '12':
#         astro_sign = 'Sagittaire' if (day < 22) else 'Capricorne'
#     elif month == '01':
#         astro_sign = 'Capricorne' if (day < 20) else 'Verseau'
#     elif month == '02':
#         astro_sign = 'Verseau' if (day < 19) else 'Poisson'
#     elif month == '03':
#         astro_sign = 'Poisson' if (day < 21) else 'Belier'
#     elif month == '04':
#         astro_sign = 'Belier' if (day < 20) else 'Taureau'
#     elif month == '05':
#         astro_sign = 'Taureau' if (day < 21) else 'Gemeaux'
#     elif month == '06':
#         astro_sign = 'Gemeaux' if (day < 21) else 'Cancer'
#     elif month == '07':
#         astro_sign = 'Cancer' if (day < 23) else 'Lion'
#     elif month == '08':
#         astro_sign = 'Lion' if (day < 23) else 'Vierge'
#     elif month == '09':
#         astro_sign = 'Vierge' if (day < 23) else 'Balance'
#     elif month == '10':
#         astro_sign = 'Balance' if (day < 23) else 'Scorpion'
#     elif month == '11':
#         astro_sign = 'Scorpion' if (day < 22) else 'Sagittaire'
#     return astro_sign


# Traitement de la localisation
def Trait_loc(idCommune, sNomCommune, sPays):
    #
    print("TEST_LOC function")
    Test_loc = check_localisation(Sess_deces, idCommune, sNomCommune, sPays)
    print(f"valeur de : {Test_loc}")
    if not Test_loc:
        #
        print("Rentre de la FALSE")
        CheckLieu = get_localisation_info(idCommune + ', ' + sPays)
        if CheckLieu is not None:
            print(f"Passe le test CheckLieu : {CheckLieu['place_id']}")
            Check_place_id = check_placeid(Sess_deces, CheckLieu['place_id'])
            if not Check_place_id:
                print("@@@@ PLACEID ABSENT @@@@")
                place_id = create_localisation(Sess_deces, CheckLieu['place_id'], CheckLieu['display_name'], idCommune,
                                               sNomCommune, '', sPays, CheckLieu['lat'], CheckLieu['lon'])
                print(f"place_id --> {place_id}")
            else:
                # pass
                print("### PLACEID PRESENT ###")
                print(f"place_id --> {place_id}")
                exit(1)
    else:
        print("*** IDCOMM PRESENT ***")

def update_records(subset):
    """Fonction exécutée par chaque processus pour mettre à jour un sous-ensemble de données."""

    print("update_records")
    #print(subset)

    # Créer une session propre à chaque processus
    Session_update = Sessions()
    Session_update.execute(text("PRAGMA journal_mode=WAL;"))
    Session_update.commit()

    try:
        for row in subset:
            Session_update.query(Personne).filter(Personne.dd_code_commune == row["dd_code_commune"]).update(
                {"dd_lib_commune": row["dd_lib_commune"], "dd_pays": row["dd_pays"]}
            )
        Session_update.commit()
    except Exception as e:
        Session_update.rollback()
        print(f"Erreur lors de la mise à jour du subset {subset}: {e}")
    finally:
        Session_update.close()

def Update_CITYDD(sSession):
    """MAJ de la colonne personnes.dd_lib_commune, dd_pays
    """

    # try:
    #     # Essayer de récupérer un enregistrement dans la table Personne pour tester la session
    #     sSession.query(Personne).first()
    #     print("La session est fonctionnelle.")
    # except Exception as e:
    #     print("La session n'est pas fonctionnelle :", e)

    # VILLE DD FRANCAISE : Utiliser la méthode query avec jointure et conditions WHERE
    results = sSession.query(FRANCE_LOC.Code_INSEE, FRANCE_LOC.Commune).join(Personne,
                                                                             FRANCE_LOC.Code_INSEE == Personne.dd_code_commune). \
        filter(FRANCE_LOC.Commune != Personne.dd_lib_commune).distinct().order_by(FRANCE_LOC.Code_INSEE).all()

    #print(str(results.statement.compile(compile_kwargs={"literal_binds": True})))


    # Parcourir les résultats pour mettre à jour la table 'personnes'
    print("Maj en base dd_lib_commune francaise")
    # for row in results:
    #     print(f"{row.Code_INSEE}")
    #     sSession.query(Personne).filter(Personne.dd_code_commune == row.Code_INSEE).update(
    #         {"dd_lib_commune": row.Commune, "dd_pays": "FRANCE"})
    #
    #     # Confirmer les modifications
    #
    #     sSession.commit()
    if not results :
        print("Aucune mise à jour.")
    else :
        #print("ici")
        # Préparer une liste de mises à jour
        updates = [
            {"dd_code_commune": row.Code_INSEE, "dd_lib_commune": row.Commune, "dd_pays": "FRANCE"}
            for row in results
        ]

        print("Mise à jour en base des libellés de commune Française")

        # Diviser les mises à jour en sous-ensembles
        subset_size = 10  # Taille de chaque sous-ensemble (à ajuster selon votre dataset)
        subsets = np.array_split(updates, len(updates) // subset_size)

        #print(subsets[0])
        # Effectuer la mise à jour en parallèle
        with ProcessPoolExecutor() as executor:
            executor.map(update_records, subsets)

        print("Mise à jour terminée.")

    # À la fin de votre script, avant de fermer la session
    sSession.execute(text("PRAGMA journal_mode=DELETE;"))


    # VILLE DD ETRANGERE : Utiliser la méthode query avec jointure et conditions WHERE
    results = sSession.query(COUNTRIES_LOC.Code_INSEE, COUNTRIES_LOC.Country, COUNTRIES_LOC.Capital,
                             COUNTRIES_LOC.Continent).join(Personne,
                                                           COUNTRIES_LOC.Code_INSEE == Personne.dd_code_commune). \
        filter(Personne.dd_lib_commune == '_').distinct().order_by(COUNTRIES_LOC.Code_INSEE).all()

    # Parcourir les résultats pour mettre à jour la table 'personnes'
    print("Maj en base dd_lib_commune etrangere")
    for row in results:
        print(f"{row.Code_INSEE}")
        sSession.query(Personne).filter(Personne.dd_code_commune == row.Code_INSEE).update(
            {"dd_lib_commune": row.Capital.capitalize(), "dd_pays": row.Country, "dd_continent": row.Continent})

    # Confirmer les modifications
    sSession.commit()


# Créer la fonction pour supprimer les enregistrements en fonction de l'année
def del_records_by_year(year):


    # Créer une session propre à chaque processus
    Session_delete = Sessions()
    Session_delete.execute(text("PRAGMA journal_mode=WAL;"))
    Session_delete.commit()

    try:
        # Supprimer les enregistrements de la première table en fonction de l'année
        Session_delete.query(Personne).filter(Personne.dd_an == year).delete()

        # Supprimer les enregistrements de la deuxième table en fonction de l'année
        Session_delete.query(Prenoms).filter(Prenoms.fichier == year).delete()

        # Valider les changements
        Session_delete.commit()

        print("Les enregistrements pour l'année", year, "ont été supprimés avec succès.")

    except Exception as e:
        print("Erreur lors de la suppression des enregistrements :", str(e))
    finally:
        Session_delete.close()


def csv_to_list(file_path):
    data_list = []  # Liste pour stocker les données du CSV
    with open(file_path) as csvfile:
        reader = csv.reader(csvfile, delimiter=';')
        headers = next(reader)  # Lire la première ligne comme en-têtes

        for row in reader:
            #my_record = eval("{" + ', '.join([f'\"{headers[i]}\": \"{row[i]}\"' if i in iStringPersonnes else f'\"{headers[i]}\": {row[i]}' for i in range(len(headers))]) +"}")
            my_record = eval("{" + ', '.join([f'\"{headers[i]}\": \"{row[i]}\"'  for i in range(len(headers))]) + "}")
            #my_record = { headers[i]: row[i] for i in range(len(headers)) }
            #my_record = ', '.join([f"'{row[i]}'" if i in iStringPersonnes else f"{row[i]}" for i in range(len(headers))])
            data_list.append(my_record)  # Ajouter chaque enregistrement

    return data_list

####################
#### Create CSV ####
####################
def CreateCSV(strNom, strPrenom, iSexe, iDBAnnee, iDBMois, iDBJour, iDBCommune, sDBCommune, sDBPays, iDDAnnee, iDDMois,
              iDDJour, iDDCommune):
    print(f"#### CREATE CSV {strNom} {strPrenom} ####")

    # Ecriture en base
    print("Record absent dans la database.")
    lapersonne = Personne(nom=strNom,
                          prenom=strPrenom,
                          sexe=iSexe,
                          age=iAge,
                          long_nom=len(strNom),
                          nbre_prenoms=icptPrenom,
                          db_an=iDBAnnee,
                          db_mois=iDBMois,
                          db_jour=iDBJour,
                          db_lib_jour=sDBJour,
                          db_week=iDBWeek,
                          db_code_commune=iDBCommune,
                          db_lib_commune=sDBCommune,
                          db_code_dpt=iDBCommune[0:2],
                          db_pays=sDBPays,
                          dd_an=iDDAnnee,
                          dd_mois=iDDMois,
                          dd_jour=iDDJour,
                          dd_lib_jour=sDDJour,
                          dd_week=iDDWeek,
                          dd_code_commune=iDDCommune,
                          dd_lib_commune=sDDCommune,
                          dd_code_dpt=iDDCommune[0:2],
                          iso_dpt=iSameDPT,
                          iso_commune=iSameCOM,
                          distance=iDist)

    arrPrenoms = strPrenom.split(" ")
    print(f"arrPrenoms : {arrPrenoms}")
    # print("Prenom principal : " + arrPrenoms[0])
    # cles Prenom principal : DB-P-SEXE-ANNEE DB -MOIS DB-PRENOM ou DD-P-SEXE-ANNEE DD-MOIS DD-PRENOM
    # arrCles=["DB*P*" + iSexe + "*" + iDBAnnee + "*" + iDBMois + "*" + arrPrenoms[0] ,"DD*P*" + iSexe + "*" + iDDAnnee + "*" + iDDMois + "*" + arrPrenoms[0]]
    arrCles = [
        {"evt": "DB", "level": "P", "sexe": iSexe, "annee": iDBAnnee, "mois": iDBMois, "jour": iDBJour,
         "code_dpt": iDBCommune[0:2], "prenom": arrPrenoms[0]},
        {"evt": "DD", "level": "P", "sexe": iSexe, "annee": iDDAnnee, "mois": iDDAnnee, "jour": iDDJour,
         "code_dpt": iDDCommune[0:2], "prenom": arrPrenoms[0]},
    ]
    print(arrCles)
    # Ajout en base
    instances_prenoms = [Prenoms(**data) for data in arrCles]
    Sess_deces.add_all(instances_prenoms)

    # Enregistrez les modifications dans la base de données
    Sess_deces.commit()

    i = 0
    for autrePrenom in arrPrenoms:
        print("liste des prenom : " + autrePrenom)
        if i > 0:
            print("prenom secondaire a ecrire  : " + autrePrenom)
            # cles Prenom secondaire : DB-S-SEXE-ANNEE DB -MOIS DB-PRENOM ou DD-S-SEXE-ANNEE DD-MOIS DD-PRENOM
            # arrCles=["DB*S*" + iSexe + "*" + iDBAnnee + "*" + iDBMois + "*" + autrePrenom , "DD*S*" + iSexe + "*" + iDDAnnee + "*" + iDDMois + "*" + autrePrenom]
            arrCles = [
                {"evt": "DB", "level": "S", "sexe": iSexe, "annee": iDBAnnee, "mois": iDBMois, "jour": iDBJour,
                 "code_dpt": iDBCommune[0:2],
                 "prenom": autrePrenom},
                {"evt": "DD", "level": "S", "sexe": iSexe, "annee": iDDAnnee, "mois": iDDAnnee, "jour": iDDJour,
                 "code_dpt": iDDCommune[0:2],
                 "prenom": autrePrenom},
            ]
            print(arrCles)
            # Ajout en baseB
            instances_prenoms = [Prenoms(**data) for data in arrCles]
            Sess_deces.add_all(instances_prenoms)
            # Enregistrez les modifications dans la base de données
            Sess_deces.commit()
        i = 1


#############################
#### CHECK PERSON RECORD ####
#############################
def CheckPersonne(strNom, strPrenom, iSexe, iDBAnnee, iDBMois, iDBJour, iDBCommune, sDBCommune, sDBPays, iDDAnnee,
                  iDDMois, iDDJour, iDDCommune):
    print(f"#### CHECK PERSON RECORD : {strNom} {strPrenom} ####")
    i = "0"
    while i == "0":
        # jours deces et naissance
        if iDDJour == "00":
            # print("DD Correction JOUR " + iDDJour)
            iDDJour = str(random.randint(1, 28))
            if len(iDDJour) == 1:
                iDDJour = "0" + iDDJour
        if iDDMois == "00":
            # print("DD Correction MOIS " + iDDMois)
            iDDMois = random.randint(1, 12)
            if iDDMois == 2:
                iDDMois = iDDMois + random.randint(1, 10)
            iDDMois = str(iDDMois)
            if len(iDDMois) == 1:
                iDDMois = "0" + iDDMois
        if iDDMois == "02" and iDDJour == "29":
            iDDMois = "03"
        try:
            sDDJour = days[calendar.weekday(int(iDDAnnee), int(iDDMois), int(iDDJour))]
            # print(f"sDDJour : {sDDJour}")
            iDDWeek = datetime(int(iDDAnnee), int(iDDMois), int(iDDJour)).isocalendar()[1]
            # print(f"iDDWeek : {iDDWeek}")
            break
        except ValueError:
            print("### DD Probleme avec ce jour : " + iDDJour)
            print("et avec ce mois : " + iDDMois)
            print("et avec cette annee : " + iDDAnnee)
            print(f"nom et prenom :  {strNom} {strPrenom} ")

            # wait = input("Press Enter to continue.")
            exit()
    z = "0"
    while z == "0":
        try:
            if iDBJour == "00":
                # print("DB Correction JOUR " + iDBJour)
                iDBJour = str(random.randint(1, 28))
                if len(iDBJour) == 1:
                    iDBJour = "0" + iDBJour
            if iDBMois == "00":
                # print("DB Correction MOIS " + iDBMois)
                iDBMois = random.randint(1, 12)
                if iDBMois == 2:
                    iDBMois = iDBMois + random.randint(1, 10)
                iDBMois = str(iDBMois)
                if len(iDBMois) == 1:
                    iDBMois = "0" + iDBMois
            if iDBMois == "02" and iDBJour == "29":
                iDBMois = "03"
            sDBJour = days[calendar.weekday(int(iDBAnnee), int(iDBMois), int(iDBJour))]
            # print(f"sDBJour : {sDBJour}")
            iDBWeek = datetime(int(iDBAnnee), int(iDBMois), int(iDBJour)).isocalendar()[1]
            # print(f"iDBWeek : {iDBWeek}")
            break
        except ValueError:
            print("### DB " + strNom + ", " + strPrenom)
            print("### DB Probleme avec cette date : " + iDBJour + "," + iDBMois + "," + iDBAnnee)
            exit()

    print("Appel get_personne()")
    personne_instance = get_personne(Sess_deces, iDBAnnee, iDBMois, iDBJour, strNom, strPrenom, iSexe)
    # print(f"personne_instance : {personne_instance}")
    if personne_instance == 1:
        pass
    else:
        print("Record absent dans la database.")
        print(f"{strNom}, {strPrenom}, {iSexe} : {iDBJour} / {iDBMois} / {iDBAnnee} ")
        f = open('CHECK_PERSON_' + iAnnee + '.txt', 'a')
        f.write(f"{strNom}, {strPrenom}, {iSexe} : {iDBJour} / {iDBMois} / {iDBAnnee}\n")
        f.close()


def testFichier(sNomFichier):
    if os.path.isfile(sNomFichier):
        return True
    else:
        return False


def ParseTXT(leFicIN, src):
    """Fonction de traitement.
    """

    #
    #print(mois_avec_31_jours)
    #print(type(leFicIN))
    #print(f" fichier source : {leFicIN}")
    #print("looooooooooooooo")
    match = re.search(r'/(\d{4})/', str(leFicIN))
    #print("laaaaaaaaaaaaaaa")
    if match:
        iAnnee = match.group(1)
        #print("Année extraite :", iAnnee)
    else:
        print("Aucune année trouvée dans la chaîne.")
        exit(1)

    fPersonnesCSV = "../DATAS/" + iAnnee + "/OUT/PERSONNES/personne_" + os.path.basename(leFicIN)[6:] + ".csv"
    fPrenomsCSV = "../DATAS/" + iAnnee + "/OUT/PRENOMS/prenom_" + os.path.basename(leFicIN)[6:] + ".csv"


    # Listes
    PersonnesToCSV = []
    PrenomsToCSV = []

    # les données
    # print("------------------------")
    for ligne in src:
        DataLine = []
        strNom = ligne[0]
        strPrenom = ligne[1]
        #print(f"---> nom et prenom : {strNom}, {strPrenom} <----")

        try:
            iSexe = ligne[2]
            #print(iSexe)
            iDBAnnee = ligne[3][:4]

            iDBMois = ligne[3][4:6]
            iDBJour = ligne[3][6:]
            # print(f"Info naiss : {iDBAnnee}, {iDBMois}, {iDBJour} <--> {ligne[3]}")
            iDBCommune = ligne[4]
            #print(f"Voici la commune de naissance : {iDBCommune} et le type : {type(iDBCommune)}")
            #print("CP : " +  iDBCommune )
            # testFichier(sFichierCOMMUNE)
            sDBCommune = ligne[5]
            # if iDBCommune[0:2] == '75' and sDBCommune[0:5] == 'PARIS':
            #    sDBCommune="PARIS"
            #print("COMMUNE : " +  sDBCommune )
            # print(len(sDBCommune))
            sDBPays = ligne[6]
            sDBContinent = 'EUROPE'
            if sDBPays == "FRANCE METROPOLITAINE":
                sDBPays = "FRANCE"

            if iDBCommune[0:2] == '99':
                sDBPays = dict_coord_foreign.get(iDBCommune, {}).get('Country')
                sDBContinent = dict_coord_foreign.get(iDBCommune, {}).get('Continent')
                iCoordDB=dict_coord_foreign.get(iDBCommune, {}).get('geo_point_2d')
                if iCoordDB is None:
                    dict_coord_unknown[iDBCommune] = {'Commune': sDBCommune, 'Pays': sDBPays, 'Fichier': iAnnee}
                    #print(f"La commune {iDBCommune} n'a pas de coordonnées")
                #print(f"Les coord DB Foreign sont : {iCoordDB}")
                #print(f"Naissance Voici les 2 valeurs : {sDBPays}, {sDBContinent}")
            else:
                iCoordDB = dict_coord_france.get(iDBCommune, {}).get('geo_point_2d')
                if iCoordDB is None:
                    dict_coord_unknown[iDBCommune] = {'Commune': sDBCommune, 'Pays': sDBPays, 'Fichier': iAnnee}

                #print(f"Les coord DB France sont : {iCoordDB}")


            #print ("PAYS NAISS : *" +  sDBPays + "*")
            iDDAnnee = ligne[7][:4]
            #print ("Annee mort :  " +  iDDAnnee)
            iDDMois = ligne[7][4:6]
            #print ("Mois mort :  " +  iDDMois)
            iDDJour = ligne[7][6:]
            #print ("Jour mort :  " +  iDDJour)
            iDDCommune = ligne[8]


            # print("ici")
            #i = "0"
            #while i == "0":
            # jours deces et naissance
            iDDJour, iDDMois, iDDAnnee = correction_date(iDDJour, iDDMois, iDDAnnee)
            try:
                sDDJour = days[calendar.weekday(int(iDDAnnee), int(iDDMois), int(iDDJour))]
                # print(f"sDDJour : {sDDJour}")
                iDDWeek = datetime(int(iDDAnnee), int(iDDMois), int(iDDJour)).isocalendar()[1]
                # print(f"iDDWeek : {iDDWeek}")
                #break
            except ValueError:
                print("### DD Probleme avec ce jour : " + iDDJour)
                print("et avec ce mois : " + iDDMois)
                print("et avec cette annee : " + iDDAnnee)
                print(f"nom et prenom :  {strNom} {strPrenom} ")

                # wait = input("Press Enter to continue.")
                exit()

            #print(f"Le nom et prenom est : {strNom} {strPrenom} et date DD est : {iDDJour} -- {iDDMois} -- {iDDAnnee}")
            #z = "0"
            #while z == "0":
            #try:
            sDateOrigine = f"{iDBJour} / {iDBMois} / {iDBAnnee}"
            #print(f"Types de dates : {type(iDBJour)}, {type(iDBMois)}, {type(iDBAnnee)}")

            #print(f"La date DB origine est : {sDateOrigine}")
            # Correction du mois si la valeur est "00"
            iDBJour, iDBMois, iDBAnnee = correction_date(iDBJour, iDBMois, iDBAnnee)
            #print(f"La date DB est : {iDBJour} / {iDBMois} / {iDBAnnee}")
            try:
                #print(f"2 / Le jour est : {iDBJour}")
                sDBJour = days[calendar.weekday(int(iDBAnnee), int(iDBMois), int(iDBJour))]
                # print(f"sDBJour : {sDBJour}")
                iDBWeek = datetime(int(iDBAnnee), int(iDBMois), int(iDBJour)).isocalendar()[1]
                # print(f"iDBWeek : {iDBWeek}")
                #break
            except ValueError:
                print("### DB " + strNom + ", " + strPrenom)
                print(f"{sDateOrigine}")
                print("### DB Probleme avec cette date : " + iDBJour + "," + iDBMois + "," + iDBAnnee)
                exit()

            #print(f"Le nom et prenom est : {strNom} {strPrenom} et date DB est : {iDBJour} / {iDBMois} / {iDBAnnee}")
            # Age
            d0 = date(int(iDBAnnee), int(iDBMois), int(iDBJour))
            d1 = date(int(iDDAnnee), int(iDDMois), int(iDDJour))
            delta = d1 - d0
            iAge = int(delta.days / 365.25)
            #print("--> Age : " + str(iAge))

            # Nbre de prenom
            #icptPrenom = 0
            icptPrenom = strPrenom.count(',') + 1

            #print(f"Prenoms : {strPrenom} / {icptPrenom}")

            # Signe zodiaque
            #sZodiaque = zodiac_sign(int(iDBJour.replace("0", "")), iDBMois)
            # print(sZodiaque)

            # Meme DPT en DB et DD
            iSameDPT = "1"
            if iDBCommune[0:2] != iDDCommune[0:2]:
                iSameDPT = "0"
            # print(iSameDPT)

            #
            sDDCommune = "_"
            # print(sDDCommune)

            # Test si jour et mois naissance identique a deces
            iIsoDate=0
            if iDBJour == iDDJour and iDBMois == iDDMois:
                iIsoDate = 1


            # Pays et continent du deces
            sDDPays = 'FRANCE'
            sDDContinent = 'EUROPE'
            if iDDCommune[0:2] == "99" :
                sDDPays = dict_coord_foreign.get(iDDCommune, {}).get('Country')
                sDDContinent = dict_coord_foreign.get(iDDCommune, {}).get('Continent')
                iCoordDD = dict_coord_foreign.get(iDDCommune, {}).get('geo_point_2d')
                sDDCommune = dict_coord_foreign.get(iDDCommune, {}).get('Capital')
                if iCoordDD is None:
                    dict_coord_unknown[iDDCommune] = {'Commune': sDDCommune, 'Pays': sDDPays, 'Fichier': iAnnee}
                    print(f"Pour le nom et prenom : {strNom} {strPrenom}, {iDDCommune} --> icoordDD est None")
                    # #print(f"Commune mort Foreign:  {iDDCommune} coord DD : {iCoordDD}")
            else:
                iCoordDD = dict_coord_france.get(iDDCommune, {}).get('geo_point_2d')
                sDDCommune = dict_coord_france.get(iDDCommune, {}).get('Commune')
                if iCoordDD is None:
                    dict_coord_unknown[iDDCommune] = {'Commune': sDDCommune, 'Pays': sDDPays, 'Fichier': iAnnee}


                #if iCoordDD is None:
                    #print(f"Pour le nom et prenom : {strNom} {strPrenom}, {iDDCommune} --> icoordDD est None")

                #print(f"Commune mort France:  {iDDCommune} et corrd DD : {iCoordDD}")

            # Calcul distance entre commune naissance et deces
            iDist = 123456789.987654
            iSameCOM = 1
            if iDBCommune != iDDCommune :
                iSameCOM = 0
                if iCoordDD is not None and iCoordDB is not None:
                    iDist = Calc_Distance(iCoordDB, iCoordDD, f"{strPrenom} {strNom}")
            else:
                iDist=0

            # Pays pareil ?
            iIsoPays=0
            if sDDPays == sDBPays:
                iIsoPays=1

            # print(str(iDist))


            #print("ICI")
            # Traitement Personne
            # Ajout Personne variables dans la liste
            lignepersonnecsv = [strNom, strPrenom, iSexe, iAge, len(strNom), icptPrenom, iDBAnnee, iDBMois,
                                iDBJour, sDBJour, iDBWeek, iDBCommune, sDBCommune, iDBCommune[0:2], sDBPays, sDBContinent, iDDAnnee, iDDMois,
                                iDDJour, sDDJour, iDDWeek, iDDCommune, sDDCommune, iDDCommune[0:2], sDDPays, sDDContinent, iIsoDate, iSameDPT, iSameCOM, iIsoPays, iDist]

            #print(f"Datas : {lignepersonnecsv}")
            # Ajout liste dans la liste generale des personnes
            PersonnesToCSV.append(lignepersonnecsv)

            # Traitement Prenom
            arrprenoms = strPrenom.split(",")
            #
            ligneprenomcsv = ['DB', 'P', iSexe, iDBAnnee, iDBMois, iDBJour, iDBCommune[0:2], arrprenoms[0],iDDAnnee]
            PrenomsToCSV.append(ligneprenomcsv)
            #
            ligneprenomcsv = ["DD", "P", iSexe, iDDAnnee, iDDMois, iDDJour, iDDCommune[0:2], arrprenoms[0],iDDAnnee]
            PrenomsToCSV.append(ligneprenomcsv)

            i = 0
            for autreprenom in arrprenoms:
                if i > 0:
                    #
                    ligneprenomcsv = ['DB', 'S', iSexe, iDBAnnee, iDBMois, iDBJour, iDBCommune[0:2], autreprenom,iDDAnnee]
                    PrenomsToCSV.append(ligneprenomcsv)
                    #
                    ligneprenomcsv = ["DD", "S", iSexe, iDDAnnee, iDDMois, iDDJour, iDDCommune[0:2], autreprenom,iDDAnnee]
                    PrenomsToCSV.append(ligneprenomcsv)
                i = 1
            #
        except Exception as e:
            print("Pb de traitement, l'erreur est la suivante : {e}")
            traceback.print_exc()
            print(f"---> nom et prenom : {strNom}, {strPrenom} <----")
            print(f"Commune : {iDBCommune} / {sDBCommune}, Les coord DB  sont : {iCoordDB}")
            print(f"Commune : {iDDCommune} / {sDDCommune}, Les coord DD  sont : {iCoordDD}")
            print(f"La Distance est : {iDist}")

    # Ecriture des Communes sans coord geo dans la table UNKNOWN_LOC
    #print(f"Nombre d'enregistrements à inserer dans la table UNKNOWN_LOC : {len(dict_coord_unknown)}")
    ret = inserer_dans_unknown_loc(dict_coord_unknown)

    # Ecriture de la liste des personnes dans le fichier final en une seule fois
    #print(f"Ecriture dans : {fPersonnesCSV} ")
    with open(fPersonnesCSV, 'w') as fPersonnes:
        write = csv.writer(fPersonnes, delimiter=';', dialect='unix')
        write.writerow(sEnTetePersonnes)
        write.writerows(PersonnesToCSV)
    #
    # Ecriture de la liste des prenoms dans le fichier final en une seule fois
    #print(f"Ecriture dans : {fPrenomsCSV} ")
    with open(fPrenomsCSV, 'w') as fPrenoms:
        write = csv.writer(fPrenoms, delimiter=';', dialect='unix')
        write.writerow(sEnTetePrenoms)
        write.writerows(PrenomsToCSV)

def thread_fichier_Prepare(fFichierIN):
    # Ouverture du fichier source
    #print("ICI")
    #print(f"*** FICHIER : {fFichierIN}")

    if os.path.isfile(fFichierIN):
        csv_file = open(fFichierIN)
        source = csv.reader(csv_file, delimiter=';')
        ParseTXT(fFichierIN, source)
    else:
        print("\n #### FICHIER ABSENT : \n")
        print(fFichierIN)



def inject_csv_to_Personne(fCSV):

    global ChunkSize

    #print(fCSV)
    data = csv_to_list(fCSV)


    # Créer une session propre à chaque processus
    Session_insert_personnes = Sessions()
    #Session_insert_personnes.execute(text("PRAGMA journal_mode=WAL"))
    #Session_insert_personnes.commit()


    for i in range(0, len(data), ChunkSize):
        chunk = data[i:i + ChunkSize]
        # Utiliser executemany pour l'insertion en masse
        try:
            Session_insert_personnes.execute(Personne.__table__.insert(), chunk)
            retry_commit(Session_insert_personnes)
            #Session_insert_personnes.commit()
            # try:
            #     retry_commit(Session_insert)
            # except OperationalError as e:
            #     print(f"Erreur persistante : {e}")

        except IntegrityError as e:
            print(f"Erreur d'intégrité pour {fCSV}: {e}")

        except OperationalError as e:
            print(f"Erreur opérationnelle (base verrouillée ?) pour {fCSV}: {e}")

        except SQLAlchemyError as e:
            print(f"Erreur SQLAlchemy pour {fCSV}: {e}")
            Session_insert_personnes.rollback()

        finally:
            Session_insert_personnes.close()
            #progress_bar.update(1)  # Met à jour la barre de progression

def inject_csv_to_Prenoms(fCSV):
    #
    #print(fCSV)
    #
    data = csv_to_list(fCSV)


    # Créer une session propre à chaque processus
    Session_insert_prenoms = Sessions()
    #Session_insert_prenoms.execute(text("PRAGMA journal_mode=WAL"))
    #Session_insert_prenoms.commit()


    # Utiliser executemany pour l'insertion en masse
    try:
        Session_insert_prenoms.execute(Prenoms.__table__.insert(), data)
        Session_insert_prenoms.commit()
    except SQLAlchemyError as e:
        print(fCSV)
        print(f"PB inject prenoms : {e}")
    finally:
        Session_insert_prenoms.close()
        #progress_bar.update(1)  # Met à jour la barre de progression

def traitement(iAnnee):
    #
    global csvwriter, fBAD, fNOMBAD, conn, communes_pb, config_yaml
    print("\n*** TRAITEMENT ANNEE : " + iAnnee)


    # Preparation des chemins
    sPath="../DATAS/" + iAnnee
    sPathSource = sPath + '/SOURCE'
    sPathIN = sPath+ '/IN'
    sPathOUT = sPath+ '/OUT'
    sPathOUTPers = sPath + '/OUT/PERSONNES'
    sPathOUTPren = sPath + '/OUT/PRENOMS'


    if args.action == "PREPARE_DIR":

        print(f"Action : {args.action} pour l'année {iAnnee}")

        # Démarrer le timer
        start_time = time.time()

        sDestNode=f"traitement.annee_{iAnnee}.{args.action}".lower()
        # Lecture des parametres Parquet du fichier Yaml
        try:
            # Parquet
            parquet_path = get_yaml_value(config_yaml, "main.parquet.parquet_path", required=True)
            parquet_source = get_yaml_value(config_yaml, "main.parquet.parquet_source", required=True)
            parquet_date = get_yaml_value(config_yaml, "main.parquet.parquet_date" )
            parquet_prefix = get_yaml_value(config_yaml, "main.parquet.parquet_prefix", required=True)
            # Section Annee
            #fichier_annee_date = get_yaml_value(config_yaml, iAnnee, "date")
        except MissingKeyError as e:
            print(e)
            exit(1)

        # Lecture de la date du fichier parquet source
        sfic_parquet=f"{parquet_path}/{parquet_source}"
        date_fic_parquet=get_creation_date_fic(sfic_parquet)
        #print(f"Date du fichier parquet source : {date_fic_parquet} : {type(date_fic_parquet)}") #, date_fic_parquet)


        # Lecture de la date du fichier parquet source
        iTestDateParquet = 0
        #
        if parquet_date is None:
            config_yaml=set_yaml_value(config_yaml, "main.parquet.parquet_date", date_fic_parquet)


        # Lecture de la date du fichier csv source
        fFICHIERANNEE=f"{sPathSource}/{parquet_prefix}{iAnnee}.csv"
        date_fic_csv="01/01/1970"
        if os.path.exists(fFICHIERANNEE):
            print(f"Fichier csv source : {fFICHIERANNEE}")
            date_fic_csv=get_creation_date_fic(fFICHIERANNEE)
        #print(f"Date du fichier csv source : {date_fic_csv} : {type(date_fic_csv)}") #, date_fic_csv)

        # Actions sur les répertoires Annee/IN et Annee/OUT et Creation a vide
        delete_directories(sPathIN, sPathOUTPers, sPathOUTPren)
        create_directories(sPathIN, sPathOUTPers, sPathOUTPren, sPathSource)

        # Utilisation du fichier parquet total ou de l'annee si deja traitée
        sFichierParquetAnnee= f"{parquet_path}/{parquet_prefix}{iAnnee}.parquet"
        #print(sFichierParquetAnnee)
        if os.path.exists(sFichierParquetAnnee):
            bDate_compare = date_superieur(date_fic_csv, date_fic_parquet)
            #print("Charge du fichier parquet annee")
            # Charger seulement les colonnes nécessaires avec pyarrow
            df_filtered = pd.read_parquet(sFichierParquetAnnee)

        else:
            #print("Charger du gros fichier parquet")
            # Charger seulement les colonnes nécessaires avec pyarrow
            df = pd.read_parquet(sfic_parquet)

            # supprimer les colonnes
            # Exclure certaines colonnes
            df = df.drop(columns=["numero_acte_deces", "fichier_origine","opposition"])

            # Filtrer les lignes où `date_naissance` commence par iAnne
            df_filtered = df[df['date_deces'].astype(str).str.startswith(iAnnee)]

            # Convertir le DataFrame en Table Arrow et écrire dans un fichier Parquet
            table = pa.Table.from_pandas(df_filtered)
            pq.write_table(table, sFichierParquetAnnee)

        # Lecture de la date du fichier parquet annee
        date_fic_annee=get_creation_date_fic(sFichierParquetAnnee)
        config_yaml = set_yaml_value(config_yaml, f"{sDestNode}.parquet_date", date_fic_annee)

        # Appliquer la correction sur la colonne prenoms
        df_filtered.loc[:,'prenoms'] = df_filtered['prenoms'].apply(correct_surnames)

        # Appliquer la correction sur la colonne noms
        df_filtered.loc[:,'nom'] = df_filtered['nom'].apply(correct_names)

        # Correction inversion nom et prenom
        df_filtered = corriger_nom_prenom(df_filtered, noms_prenoms_dict)

        # Exporter le résultat vers un fichier CSV
        df_filtered.to_csv(fFICHIERANNEE, sep=';', index=False, header=False, escapechar='\\', encoding='utf-8')

        # creation fichiers a traiter
        #print("Split des fichiers")
        sCde = 'split -l ' + str(splitLimite) + ' -d ' + fFICHIERANNEE + ' ' + sPathIN + '/trait_'
        ret = subprocess.call(sCde, shell=True)

        # Création des fichiers à traiter et comptage des fichiers générés
        sCde = f"split -l {splitLimite} -d {fFICHIERANNEE} {sPathIN}/trait_ && ls {sPathIN}/trait_* | wc -l"
        result = subprocess.run(sCde, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        if result.returncode == 0:
            file_count = int(result.stdout.strip())  # Récupérer le nombre de fichiers
            # print(f"Nombre de fichiers générés : {file_count}")
        else:
            # print(f"Erreur lors de l'exécution de la commande split : {result.stderr}")
            exit(1)


        # # Extraire la colonne 'nom' et supprimer les doublons
        # df_nom = pd.DataFrame(df_filtered['nom'].drop_duplicates())
        #
        # # Filtrer les noms qui commencent par 'A'
        # df_commence_par_A = df_nom[df_nom['nom'].str.startswith('A')]
        #
        # # Calculer les similarités
        # if not df_commence_par_A.empty:
        #     similarities = detect_similar_names(df_commence_par_A, 'nom', 95)
        #     for similarity in similarities:
        #         print(similarity)

        # Arrêter le timer et calculer le temps écoulé
        end_time = time.time()
        elapsed_time = end_time - start_time

        # Compter le nombre d'enregistrements dans le dataset filtré
        nb_enregistrements = df_filtered.shape[0]  # shape[0] donne le nombre de lignes
        print(f"\n@@ Pour l'annee {iAnnee}, nbre d'enreg dans le dataset  : {nb_enregistrements} et temps d'exec : {elapsed_time:.2f} secondes")
        config_yaml = set_yaml_value(config_yaml, f"{sDestNode}.nbre_enreg", nb_enregistrements)
        config_yaml = set_yaml_value(config_yaml, f"{sDestNode}.temps_exec", elapsed_time)
        config_yaml = set_yaml_value(config_yaml, f"{sDestNode}.nbre_splits", file_count)
        config_yaml = set_yaml_value(config_yaml, f"{sDestNode}.splitsize", splitLimite)
        config_yaml = set_yaml_value(config_yaml, f"{sDestNode}.parquet_name", sFichierParquetAnnee)

        # Afficher le temps écoulé en secondes ou dans un format lisible
        #print(f"@@ Pour l'annee {iAnnee}, temps d'exécution : {elapsed_time:.2f} secondes")

    elif args.action == "PREPARE_DATAS":

        global dict_coord_france, dict_coord_foreign, dict_coord_unknown

        print(f"Action : {args.action} pour l'année {iAnnee}")

        # Démarrer le timer
        start_time = time.time()

        if not os.path.exists(sPathIN):
            print("Il faut d'abord faire PREPARE_DIR")
            exit(1)

        delete_files_in_dir(sPathOUTPers)
        delete_files_in_dir(sPathOUTPren)

        # Delete des enregs dans la table UNKNOWN_LOC
        delete_dans_unknown_loc(iAnnee)

        #listTmp = os.listdir(sPathIN)
        listTmp = Path(sPathIN)
        listFiles = [file for file in listTmp.iterdir()  if file.is_file() and file.name.startswith('trait_')]

        print(f"Pour l' annee {iAnnee}, nombre de fichiers a traiter : {len(listFiles)}")
        #
        #max_files_in_parallel = 4
        with ProcessPoolExecutor(max_workers=max_files_in_parallel) as executor:
            executor.map(thread_fichier_Prepare, listFiles)

        # Concat personnes
        interesting_files = glob.glob(sPathOUTPers + "/personne_*.csv")

        #print(f"Interesting files : {interesting_files}")
        write_headers = True
        with open(sPathOUTPers + '/CONCAT_PERSONNES_' + iAnnee + '.csv', 'w') as fout:
            writer = csv.writer(fout, delimiter=';', dialect='unix')
            for filename in interesting_files:
                #print(filename)
                with open(filename) as fin:
                    reader = csv.reader(fin, delimiter=';', dialect='unix')
                    headers = next(reader)
                    if write_headers:
                        write_headers = False  # Only write headers once.
                        writer.writerow(headers)
                    writer.writerows(reader)  # Write all remaining rows.

        
        # Concat prenoms
        interesting_files = glob.glob(sPathOUTPren + "/prenom*.csv")

        write_headers = True
        with open(sPathOUTPren + '/CONCAT_PRENOMS_' + iAnnee + '.csv', 'w') as fout:
            writer = csv.writer(fout, delimiter=';', dialect='unix')
            for filename in interesting_files:
                #concat
                with open(filename) as fin:
                    reader = csv.reader(fin, delimiter=';', dialect='unix')
                    headers = next(reader)
                    if write_headers:
                        write_headers = False  # Only write headers once.
                        writer.writerow(headers)
                    writer.writerows(reader)  # Write all remaining rows.
                # zip du fichier csv
                #zipCSV(filename)

        # Démarrer le timer
        end_time = time.time()

        elapsed_time = end_time - start_time

        # Compter le nombre d'enregistrements dans le dataset filtré
        print(f"\n@@ Pour l'annee {iAnnee}, temps d'exec : {elapsed_time:.2f} secondes")


    elif args.action == "LOAD":

        print(f"Action : {args.action} pour l'année {iAnnee}")

        fFilePersonneConcat=f"{sPathOUTPers}/CONCAT_PERSONNES_{iAnnee}.csv"
        fFilePrenomConcat=f"{sPathOUTPren}/CONCAT_PRENOMS_{iAnnee}.csv"

        if not os.path.exists(fFilePersonneConcat) or not os.path.exists(fFilePrenomConcat):
            print(f"Test du repertoire {fFilePersonneConcat} ou {fFilePrenomConcat} : KO")
            print("Il faut d'abord faire PREPARE_DATAS")
            exit(1)

        # suppresions des enregistrements existant
        del_records_by_year(iAnnee)

        # Fichiers a traiter des personnes
        list_tmp_personnes = os.listdir(sPathOUTPers)
        list_files_personnes = [sPathOUTPers + '/' + file for file in list_tmp_personnes if file.startswith('personne_') ]
        #
        sCde = 'wc -l ' + fFilePersonneConcat
        # print(sCde)
        output = subprocess.check_output(sCde, shell=True, text=True)
        resultat = re.match(r"^\d+", output)
        iTotal = int(resultat.group()) - 1
        print(f"\n*** Pour l'annee {iAnnee}, injection du fichier des noms de personnes")

        # for file in list_files_personnes:
        #     sCde = 'wc -l ' + file
        #     #print(sCde)
        #     output = subprocess.check_output(sCde, shell=True, text=True)
        #     resultat = re.match(r"^\d+", output)
        #     #print(f"Nombre d'enreg dans le fichier {file} : {int(resultat.group())}") # {output)
        #     iTotal=iTotal+int(resultat.group()) - 1
        #     inject_csv_to_Personne(file)

        # Utilisation de ProcessPoolExecutor pour charger les fichiers 'personne' en parallele
        with ThreadPoolExecutor(max_workers=max_files_in_parallel) as executor:
            executor.map(inject_csv_to_Personne, list_files_personnes)


        print(f"\n*** Nombre d'enreg total des personnes : {iTotal} pour l'annee {iAnnee} dans {len(list_files_personnes)} fichiers")
        nbre_enreg_table = Sess_deces.query(func.count(Personne.dd_an)).filter(Personne.dd_an == iAnnee).scalar()
        print(f"Nombre d'enreg dans la table 'personnes' : {nbre_enreg_table} pour l'annee {iAnnee}")

        # suppresions des enregistrements existant
        # Fichiers a traiter des prenoms
        list_tmp_prenoms = os.listdir(sPathOUTPren)
        list_files_prenoms = [sPathOUTPren + '/' + file for file in list_tmp_prenoms if file.startswith('prenom_')]
        #
        iTotal = 0
        sCde = 'wc -l ' + fFilePrenomConcat
        # print(sCde)
        output = subprocess.check_output(sCde, shell=True, text=True)
        resultat = re.match(r"^\d+", output)
        iTotal = int(resultat.group()) - 1
        print(f"\n***Pour l'annee {iAnnee}, injection du fichier des prenoms")

        # for file in list_files_prenoms:
        #     sCde = 'wc -l ' + file
        #     # print(sCde)
        #     output = subprocess.check_output(sCde, shell=True, text=True)
        #     resultat = re.match(r"^\d+", output)
        #     # print(f"Nombre d'enreg dans le fichier {file} : {int(resultat.group())}") # {output)
        #     iTotal = iTotal + int(resultat.group()) - 1
        #     inject_csv_to_Prenoms(file)

        # Utilisation de ProcessPoolExecutor pour charger les fichiers 'personne' en parallele
        #with tqdm(total=len(list_files_prenoms), desc="Injection des fichiers") as pbar:
        with ThreadPoolExecutor(max_workers=max_files_in_parallel) as executor:
            executor.map(inject_csv_to_Prenoms, list_files_prenoms, )


        print(f"\n*** Nombre d'enreg total des prenoms : {iTotal} pour l'annee {iAnnee} dans {len(list_files_prenoms)} fichiers")
        nbre_enreg_table = Sess_deces.query(func.count(Prenoms.fichier)).filter(Prenoms.fichier == iAnnee).scalar()
        print(f"Nombre d'enreg dans la table 'prenoms' : {nbre_enreg_table} pour l'annee {iAnnee}")

        #
    elif args.action == "CITYDB":

        # Ouverture du fichier source
        fFICHIERANNEE = "./DATAS/lieux-" + iAnnee + ".csv"
        print("*** FICHIER : " + fFICHIERANNEE)

        if os.path.isfile(fFICHIERANNEE):
            csv_file = open(fFICHIERANNEE)
            source = csv.reader(csv_file, delimiter=';')
            CheckCITYDB(source)
        else:
            print("\n #### FICHIER ABSENT : \n")
            print(fFICHIERANNEE)

    elif args.action == "CITYDD":
        print("\n\n#### CITYDD #### ")

        Update_CITYDD(Sess_deces)

        # Call the function to retrieve records
        # records_dd = issue_dd_commune(Sess_deces)

        # Print the retrieved records
        # for record in records_dd:
        #    print(f"\nDD Code Commune : {record[0]}")

    elif args.action == "DIST":
        #
        print("\n\n#### DIST #### ")

        # Call the function to retrieve records for the specified year
        records_distance = issue_distance(Sess_deces, iAnnee)

        # Print the retrieved records
        for record in records_distance:
            db_code_commune, dd_code_commune = record
            # print("\nDB Code Commune:", db_code_commune)
            # print("DD Code Commune:", dd_code_commune)
            #
            iDist = Calc_Distance(db_code_commune, dd_code_commune)
            update_distance(Sess_deces, db_code_commune, dd_code_commune, iDist)
            # print("Dist : ", iDist)

        # commune ayant un pb
        # Count the occurrences of each item
        item_counts = Counter(communes_pb)

        # Sort the items based on their counts (in descending order)
        sorted_items = sorted(item_counts.items(), key=lambda x: x[1], reverse=True)

        # Create a new ordered data structure (dictionary)
        ordered_data = {item[0]: item[1] for item in sorted_items}

        # Write the set to the file
        filename = "../DATAS/pb_communes_coord_" + iAnnee + ".txt"
        write_set_to_file(filename, ordered_data, Sess_deces)

    else:
        print(f"## RIEN A FAIRE POUR {args.action}")

# Fonction pour interpréter le paramètre "years"
def parse_years(years_input):
    years = []
    for year_item in years_input:
        if '-' in year_item:  # Si c'est un intervalle (ex. 1971-1981)
            start, end = map(int, year_item.split('-'))
            years.extend(str(year) for year in range(start, end + 1))
        else:  # Si c'est une année unique (ex. 1970)
            years.append(str(year_item))
    return years

# TRAITEMENT

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Welcome to the death and birth in France processing")
    parser.add_argument("--years", dest="years", nargs="+", type=str, required=True, action="store",
                        help="Year(s) to perform")
    parser.add_argument("--action", dest="action", type=str, choices=PERFORM.keys(), required=True,
                        help="Action to perform")
    args = parser.parse_args()

    #
    # lecture parametres pour le parallelisme
    try:
        max_threads_annee = get_yaml_value(config_yaml, "main.threads.max_threads_annee", required=True)
        max_files_in_parallel_one_year = get_yaml_value(config_yaml, "main.threads.max_files_in_parallel_one_year", required=True)
        max_files_in_parallel_many_year = get_yaml_value(config_yaml, "main.threads.max_files_in_parallel_many_year", required=True)
    except MissingKeyError as e:
        print(e)
        exit(1)

    # Obtenir la liste des années à traiter
    years = parse_years(args.years)

    max_files_in_parallel = max_files_in_parallel_many_year

    if len(years) == 1:
        max_files_in_parallel = max_files_in_parallel_one_year

    if args.action == "PREPARE_DATAS":

        # Chargement du dictionnaire des coordonnées geo france
        print("Chargement du dictionnaire des coordonnées geo france")
        dict_coord_france=charger_insee_geo(Sess_deces)

        # Chargement du dictionnaire des coordonnées geo foreign
        print("Chargement du dictionnaire des coordonnées geo foreign")
        dict_coord_foreign=charger_countries_geo(Sess_deces)

    print(f"\nNombre d'années à traiter : {len(years)}, \n\tmax_threads_annee : {max_threads_annee}, \n\tmax_files_in_parallel : {max_files_in_parallel}")

    # départ le timer
    start_time_gene = time.time()
    #print(start_time_gene)

    # Utilisation de ThreadPoolExecutor pour exécuter le traitement en parallèle4
    #max_threads_annee = 2
    with ThreadPoolExecutor(max_workers=max_threads_annee) as executor:
        futures = [executor.submit(traitement, annee) for annee in years]


    # Attendre la fin de toutes les tâches
    for future in futures:
        future.result()

    # SAuvegarde des données yaml
    save_yaml_to_file(sFicYaml, config_yaml)

    # Arrêter le timer et calculer le temps écoulé
    end_time_gene = time.time()
    #print(end_time_gene)

    elapsed_time_gene = end_time_gene - start_time_gene
    print(f"\n### Temps de traitement global : {elapsed_time_gene:.2f} secondes soit, en minutes, {elapsed_time_gene / 60:.2f} minutes")
