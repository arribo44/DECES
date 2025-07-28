#
import traceback
import argparse
import calendar
import csv
import glob
import multiprocessing
import threading
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from multiprocessing.dummy import Pool as ThreadPool
import concurrent.futures
import os, time, re
import random
import shutil
from pathlib import Path
import subprocess
from collections import Counter
from datetime import date, datetime, timedelta

from tqdm import tqdm
#
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from ydata_profiling import ProfileReport

#
import pickle, json

#
import yaml

#
from functools import partial

#
from pathlib import Path

from rapidfuzz import fuzz, process

from dict_transco import corrections_prenoms, corrections_noms, noms_prenoms_dict, DPTS_ISO3166, villes_a_corriger, prenoms_FlashText

from sqlalchemy_classes import Personne, Prenoms, FRANCE_LOC, COUNTRIES_LOC, UNKNOWN_LOC, retry_commit
from sqlalchemy.exc import OperationalError, SQLAlchemyError, IntegrityError

from fonctions_utiles import (correction_date,  dates_equals,    date_superieur, get_iso3166, preprocess_data,
                              generer_date_naissance_viable, generer_date_0000_viable, generer_date_deces_viable,
                              generer_pays_viable, compter_valeurs_uniques, nettoyer_ville, verifier_validite_parquet,
                              nettoyer_colonne_villes_rapide, drop_columns_pandas, drop_columns_pyarrow, detect_prenoms,
                              compter_lignes_parquet, supprimer_doublons_parquet, compute_hash_id, modifier_valeur_par_id)

from bib_yaml import load_yaml_from_file, save_yaml_to_file,  get_yaml_value, MissingKeyError, set_yaml_value
from bib_files import (create_directories, delete_directories, delete_files_in_dir, get_creation_date_fic,
                       copier_fichier, csv_to_parquet, load_pickle, save_pickle)

#
global dict_coord_france, dict_coord_foreign, dict_coord_unknown
dict_coord_unknown = {}

# Options du script
# opts = [opt for opt in sys.argv[1:] if opt.startswith("-")]
# args = [arg for arg in sys.argv[1:] if not arg.startswith("-")]

PERFORM = {
    "PREPARE_PARQUET": "Prepare the file parquet for all years",
    "PREPARE_DIR": "Prepare All the directory and files for a year",
    "PREPARE_DATAS": "Prepare All the contents in files for a year",
    "PREPARE_PRENOMS": "Retrieve and do a treatment when a personn has no first name",
    "LOAD": "load all the contents in DB form files for a year",
    "PERSON": "Check if the person is in the DB",
    "CITYDB": "Only the Birth City",
    "CITYDD": "Only the Death City",
    "DIST": "Only the records without calculated distance",
    "SQLDB": "Top SQL of the DB City unknown",
    "RAPPORT": "Generate the rapport for a year"
}

BEFORE = {
    "DUPLICATES": "Remove duplicates",
    "COLUMNS": "Drop unnecessary columns",
    "HASH": "Hashing for ID column",
    "DATES_DECES": "Nettoyage des dates de déces",
    "DATES_NAISSANCE": "Nettoyage des dates de naissance",
    "CHECK_DATES" : "Verification des dates de naissance < dates de déces",
    "COMMUNES_NAISSANCE": "Nettoyage des communes de naissance",
    "PAYS_NAISSANCE": "Nettoyage des pays",
    "VILLES_NAISSANCE": "Nettoyage des villes",
    'PRENOMS_0': "Recherche d'un prénom dans le nom et traitement de celui-ci",
    "SPLIT_PARQUET": "Split the file parquet",
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
sEnTetePersonnes = ['nom', 'prenom', 'sexe', 'age', 'jours','long_nom',
                    'nbre_prenoms', 'db_date', 'db_lib_jour',
                    'db_week', 'db_code_commune', 'db_lib_commune', 'db_code_dpt', 'db_dept_isocode_3166',
                    'db_pays', 'db_continent', 'dd_date', 'dd_lib_jour',
                    'dd_week', 'dd_code_commune', 'dd_lib_commune', 'dd_code_dpt', 'dd_dept_isocode_3166',
                    'dd_pays', 'dd_continent', 'iso_date', 'iso_dpt', 'iso_commune', 'iso_pays',
                    'distance','hash_id']
# iStringPersonnes=(0,1,6,8,9,10,12,13,14,15,16,17,18,19,20,22,23,24,25,25,26)

sEnTetePrenoms = ['evt', 'level', 'sexe', 'date', 'code_dpt', 'prenom', 'fichier']

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
        Session_delete.query(Personne).filter(func.substr(Personne.dd_date, 1, 4) == year).delete()

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
                          jours=iJours,
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
                          distance=iDist,
                          hash_id=hash_id)

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


def count_prenoms(prenom_str):
    if not prenom_str or prenom_str.strip() == '':
        return 0
    return len([p for p in prenom_str.split(',') if p.strip()])

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
            # Test si sDBCommune est numerique alors devient vide
            if sDBCommune.isnumeric():
                sDBCommune = ""

            # if iDBCommune[0:2] == '75' and sDBCommune[0:5] == 'PARIS':
            #    sDBCommune="PARIS"
            #print("COMMUNE : " +  sDBCommune )
            # print(len(sDBCommune))
            sDBPays = ligne[6]
            sDBContinent = 'EUROPE'
            if sDBPays == "FRANCE METROPOLITAINE":
                sDBPays = "FRANCE"

            sDB_dept_isocode_3166=''
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
                sDB_dept_isocode_3166=get_iso3166(iDBCommune[0:2] , DPTS_ISO3166 )

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

                # date au format YYYY-mm-dd
                sDD_Date = f"{iDDAnnee}-{iDDMois}-{iDDJour}"

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

                # date au format YYYY-mm-dd
                sDB_Date = f"{iDBAnnee}-{iDBMois}-{iDBJour}"
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

            #
            iJours=0
            if iAge< 1:
                iJours = int(delta.days)
                iAge = 0

            # print("--> Age : " + str(iAge))

            # Nbre de prenom
            icptPrenom = count_prenoms(strPrenom)

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

            #
            sDD_dept_isocode_3166=''
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
                sDD_dept_isocode_3166=get_iso3166( iDDCommune[0:2] , DPTS_ISO3166)

            # Pays et continent de naissance
            # Calcul distance entre commune naissance et deces
            iDist = -1
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

            # Hash de la personne
            hash_id=ligne[9]


            #print("ICI")
            # Traitement Personne
            # Ajout Personne variables dans la liste
            lignepersonnecsv = [strNom, strPrenom, iSexe, iAge,  iJours, len(strNom), icptPrenom, sDB_Date, sDBJour, iDBWeek, iDBCommune, sDBCommune, iDBCommune[0:2], sDB_dept_isocode_3166, sDBPays, sDBContinent,
                                sDD_Date, sDDJour, iDDWeek, iDDCommune, sDDCommune, iDDCommune[0:2], sDD_dept_isocode_3166, sDDPays, sDDContinent, iIsoDate, iSameDPT, iSameCOM, iIsoPays, iDist, hash_id]

            #print(f"Datas : {lignepersonnecsv}")
            # Ajout liste dans la liste generale des personnes
            PersonnesToCSV.append(lignepersonnecsv)

            # Traitement Prenom
            arrprenoms = strPrenom.split(",")
            #
            ligneprenomcsv = ['DB', 'P', iSexe, sDB_Date, iDBCommune[0:2], arrprenoms[0],iDDAnnee]
            PrenomsToCSV.append(ligneprenomcsv)
            #
            ligneprenomcsv = ["DD", "P", iSexe, sDD_Date, iDDCommune[0:2], arrprenoms[0],iDDAnnee]
            PrenomsToCSV.append(ligneprenomcsv)

            i = 0
            for autreprenom in arrprenoms:
                if i > 0:
                    #
                    ligneprenomcsv = ['DB', 'S', iSexe, sDB_Date, iDBCommune[0:2], autreprenom,iDDAnnee]
                    PrenomsToCSV.append(ligneprenomcsv)
                    #
                    ligneprenomcsv = ["DD", "S", iSexe, sDD_Date, iDDCommune[0:2], autreprenom,iDDAnnee]
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

# Traitement des fichiers de personne
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

# Nouvelle fonction : elle crée sa propre session à chaque appel
def inject_csv_to_Personne_threadsafe(file_path, session):
    session = session()
    try:
        inject_csv_to_Personne(session, file_path)
        session.commit()
    except Exception as e:
        print(f"Erreur pour {file_path} : {e}")
        session.rollback()
    finally:
        session.close()

def inject_csv_to_Personne(session, fCSV):

    global ChunkSize

    # print(fCSV)
    data = csv_to_list(fCSV)

    # Prétraiter les données pour convertir les dates
    data = preprocess_data(data)

    # Créer une session propre à chaque processus
    #Session_insert_personnes = session()
    #Session_insert_personnes.execute(text("PRAGMA journal_mode=WAL"))
    #Session_insert_personnes.commit()


    for i in range(0, len(data), ChunkSize):
        chunk = data[i:i + ChunkSize]
        # Utiliser executemany pour l'insertion en masse
        try:
            session.execute(Personne.__table__.insert(), chunk)
            retry_commit(session)
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
            session.rollback()

        finally:
            session.close()
            #progress_bar.update(1)  # Met à jour la barre de progression

# Nouvelle fonction : elle crée sa propre session à chaque appel
def inject_csv_to_Prenoms_threadsafe(file_path, session):
    session = session()
    try:
        inject_csv_to_Prenoms(session, file_path)
        session.commit()
    except Exception as e:
        print(f"Erreur pour {file_path} : {e}")
        session.rollback()
    finally:
        session.close()


def inject_csv_to_Prenoms(session, fCSV):
    #
    #print(fCSV)
    #
    data = csv_to_list(fCSV)

    # Créer une session propre à chaque processus
    #Session_insert_prenoms = Sessions()
    #Session_insert_prenoms.execute(text("PRAGMA journal_mode=WAL"))
    #Session_insert_prenoms.commit()


    # Utiliser executemany pour l'insertion en masse
    try:
        session.execute(Prenoms.__table__.insert(), data)
        session.commit()
    except IntegrityError as e:
        print(f"Erreur d'intégrité pour {fCSV}: {e}")
    except OperationalError as e:
        print(f"Erreur opérationnelle (base verrouillée ?) pour {fCSV}: {e}")
    except SQLAlchemyError as e:
        print(fCSV)
        print(f"PB inject prenoms : {e}")
    finally:
        session.close()
        #progress_bar.update(1)  # Met à jour la barre de progression

def traitement(iAnnee):
    #
    global csvwriter, fBAD, fNOMBAD, conn, communes_pb, config_yaml

    if not args.action == "PREPARE_PARQUET":
        print("\n*** TRAITEMENT ANNEE : " + iAnnee)

    # Preparation des chemins
    sPath="../DATAS/" + iAnnee
    sPathSource = sPath + '/SOURCE'
    sPathIN = sPath+ '/IN'
    sPathOUT = sPath+ '/OUT'
    sPathOUTPers = sPath + '/OUT/PERSONNES'
    sPathOUTPren = sPath + '/OUT/PRENOMS'


    if args.action == "PREPARE_PARQUET":

        print(f"Action : {args.action} et Before : {args.before}")

        # Démarrer le timer
        start_time = time.time()
        iHORO = datetime.now().strftime("%Y%m%d%H%M%S")

        # Node dans le yaml
        sDestNode=f"traitement.{args.action}.{args.before}.{iHORO}".lower()

        # Lecture des parametres Parquet du fichier Yaml
        try:
            # Parquet
            parquet_path = get_yaml_value(config_yaml, "main.parquet.parquet_path", required=True)
            parquet_source = get_yaml_value(config_yaml, "main.parquet.parquet_source", required=True)
            parquet_origine = get_yaml_value(config_yaml, "main.parquet.parquet_origine", required=True)
            parquet_save = get_yaml_value(config_yaml, "main.parquet.parquet_save", required=True)
            parquet_date = get_yaml_value(config_yaml, "main.parquet.parquet_date" )
            parquet_prefix = get_yaml_value(config_yaml, "main.parquet.parquet_prefix", required=True)
            parquet_rapport = get_yaml_value(config_yaml, "main.parquet.parquet_rapport_duplicates", required=True)
            csv_pays_path = get_yaml_value(config_yaml, "main.listes.pays_path", required=True)
            csv_pays_nom = get_yaml_value(config_yaml, "main.listes.pays_nom", required=True)
            csv_ville_path = get_yaml_value(config_yaml, "main.listes.ville_path", required=True)
            csv_ville_nom = get_yaml_value(config_yaml, "main.listes.ville_nom", required=True)
            pickle_path = get_yaml_value(config_yaml, "main.pickle.pickle_path", required=True)
            pickle_prenoms = get_yaml_value(config_yaml, "main.pickle.pickle_prenoms", required=True)

            # Section Annee
            #fichier_annee_date = get_yaml_value(config_yaml, iAnnee, "date")
        except MissingKeyError as e:
            print(e)
            exit(1)


        # index de l'action
        index_before = list(BEFORE.keys()).index(args.before) + 1

        # Lire le fichier Parquet original
        sfic_origine = f"{parquet_path}/{parquet_origine}"


        # Lire le fichier Parquet original
        sfic_parquet = f"{parquet_path}/{parquet_source}"

        # Fichier sauvegarde
        sfic_save=f"{parquet_path}/{parquet_save}"

        sFic_sans_extension = os.path.splitext(sfic_save)[0]
        sFic_save_etape = sFic_sans_extension + "_" + f"{index_before:03}" + "_"  + args.before + ".parquet"

        # Fichiers CSV
        sFicPaysCSV = f"{csv_pays_path}/{csv_pays_nom}"
        sFicVilleCSV = f"{csv_ville_path}/{csv_ville_nom}"

        # Fichiers Pickle des modifications diverses
        sFicPicklePrenoms = f"{pickle_path}/{pickle_prenoms}"

        if not os.path.exists(sfic_parquet):
            print(f"Le fichier {sfic_parquet} n'existe pas")
            exit(1)

        # compteur étape
        iEtape=0
        # Lire le fichier Parquet original
        #df = pd.read_parquet(sfic_parquet)

        # Suppression des colonnes
        if args.before == "DUPLICATES":

            ###########################
            # Traitement des duplicates
            ###########################

            print(f"{index_before} / Suppression des duplicates")

            fRapport = f"{csv_pays_path}/{parquet_rapport}"
            colonnes = ['nom', 'prenoms', 'sexe', 'date_naissance','code_insee_naissance','commune_naissance','pays_naissance','date_deces','code_insee_deces']

            nb = supprimer_doublons_parquet(sfic_origine,sfic_parquet,fRapport,colonnes)

            print(f"\n✅ {nb} doublons supprimés.")

        elif args.before == "COLUMNS":
            ###########################
            # Traitement des colonnes
            ###########################


            print(f"{index_before} / Suppression des colonnes via pyarrow")


            # Supprimer les colonnes
            colonnes_a_supprimer = ["numero_acte_deces", "fichier_origine", "opposition"]
            print(f" Ces colonnes : {colonnes_a_supprimer}")

            drop_columns_pyarrow(sfic_parquet, colonnes_a_supprimer)

        elif args.before == "HASH":
            ###########################
            # Traitement des colonnes
            ###########################


            print(f"{index_before} / Generation de hash pour indexation")

            # Chargement du fichier parquet
            df=pd.read_parquet(sfic_parquet)

            tqdm.pandas()  # Active la barre de progression pour pandas

            # Création de la colonne ID (hash sur 12 caractères)
            # Application avec barre de progression
            df['ID_HASH'] = df.progress_apply(compute_hash_id, axis=1)

            # Sauvegarde
            df.to_parquet(sfic_parquet, index=False)



        elif args.before == "DATES_DECES":

            # DATE DECES

            print(f"{index_before} / Traitement des dates de deces")
            # Fichier sans Extension de la sauvegarde etape
            sfic_save_etape = sFic_sans_extension + "_02_" + args.before + ".parquet"

            df=pd.read_parquet(sfic_parquet)

            # changement sur les dates de deces qui sont du type XXXX0000, on garde XXXX et on genere une date aleatoire
            print(f"-- Date de deces de type 0000")
            masque_deces_invalide = ~df["date_deces"].astype(str).str.startswith("0000") & df["date_deces"].astype(str).str.endswith("0000")
            df.loc[masque_deces_invalide, "date_deces"] = df.loc[masque_deces_invalide, "date_deces"].apply(generer_date_0000_viable)

            # changement sur les dates de deces qui sont du type XXXX0000, on garde XXXX et on genere une date aleatoire
            print(f"-- Date de deces de type 00")
            masque_deces_invalide = df["date_deces"].astype(str).str.endswith("00")
            df.loc[masque_deces_invalide, "date_deces"] = df.loc[masque_deces_invalide, "date_deces"].apply(generer_date_0000_viable)

            #
            print(f"-- Date de deces < 1800")
            masque_deces_invalide = (
                pd.to_numeric(df["date_deces"], errors="coerce") < 18000000
            )
            df.loc[masque_deces_invalide, "date_deces"] = df.loc[masque_deces_invalide, "date_deces"].apply(generer_date_deces_viable)

            # Date deces invalide
            print(f"-- Date de deces erreurs diverses comme mois 00 ou fin de date 0229 ou 0931")
            # Tentative de conversion en datetime
            df['date_deces_dt'] = pd.to_datetime(df['date_deces'], format='%Y%m%d', errors='coerce')

            # Masque des dates invalides (NaT)
            masque_dates_invalides = df['date_deces_dt'].isna()
            #print(f"masque_dates_invalides : {masque_dates_invalides}")

            # Correction uniquement sur les lignes invalides
            df.loc[masque_dates_invalides, 'date_deces'] = df.loc[masque_dates_invalides, 'date_deces'].apply(
                generer_date_deces_viable
            )

            # Suppression de la colonne date_deces_dt
            df = df.drop(columns=['date_deces_dt'])

            #
            df.to_parquet(sfic_parquet, index=False)

        elif args.before == "DATES_NAISSANCE":

            print(f"{index_before} / Traitement des dates de naissance")

            df=pd.read_parquet(sfic_parquet)

            # DATE NAISSANCE
            # changement sur les dates de naissance qui sont du type XXXX0000, on garde XXXX et on genere une date aleatoire

            print(f"-- Date de naissance de type 00000000")
            masque_naissance_invalide = df["date_naissance"] == "00000000"

            # # Appliquer la fonction sur les lignes concernées
            df.loc[masque_naissance_invalide, "date_naissance"] = df.loc[masque_naissance_invalide].apply(
                lambda row: generer_date_naissance_viable(row["date_naissance"], row["date_deces"]),
                axis=1
            )

            # changement sur les dates de naissance qui sont du type XXXX0000, on garde XXXX et on genere une date aleatoire
            print(f"-- Date de naissance fin 0000")
            masque_naissance_invalide = ~df["date_naissance"].astype(str).str.startswith("0000") & df["date_naissance"].astype(str).str.endswith("0000")
            df.loc[masque_naissance_invalide, "date_naissance"] = df.loc[masque_naissance_invalide, "date_naissance"].apply(generer_date_0000_viable)

            # # changement sur les dates de deces qui sont du type XXXX0000, on garde XXXX et on genere une date aleatoire
            print(f"-- Date de naissance fin 00")
            masque_deces_invalide = df["date_naissance"].astype(str).str.endswith("00")
            df.loc[masque_deces_invalide, "date_naissance"] = df.loc[masque_deces_invalide, "date_naissance"].apply(
                generer_date_0000_viable)


            print(f"-- Date de naissance année < 18000000")
            masque_naissance_invalide = (
                pd.to_numeric(df["date_naissance"], errors="coerce") < 18000000
            )
            df.loc[masque_naissance_invalide, "date_naissance"] = df.loc[masque_naissance_invalide].apply(
                lambda row: generer_date_naissance_viable(row["date_naissance"], row["date_deces"]),
                axis=1
            )

            year_plus_one = (datetime.now().year + 1) * 10000

            print(f"-- Date de naissance année > {year_plus_one}")
            masque_naissance_invalide = (
                    pd.to_numeric(df["date_naissance"], errors="coerce") > year_plus_one
            )
            df.loc[masque_naissance_invalide, "date_naissance"] = df.loc[masque_naissance_invalide].apply(
                lambda row: generer_date_naissance_viable(row["date_naissance"], row["date_deces"]),
                axis=1
            )

            # ICI

            # Date naissance invalide
            print(f"-- Date de naissance erreurs diverses comme mois 00 ou moisjour comme 0229 ou 0931")

            # Tentative de conversion en datetime
            df['date_nais_dt'] = pd.to_datetime(df['date_naissance'], format='%Y%m%d', errors='coerce')

            # Masque des dates invalides (NaT)
            masque_dates_invalides = df['date_nais_dt'].isna()
            print(f"masque_dates_invalides : {masque_dates_invalides}")

            # Correction uniquement sur les lignes invalides
            df.loc[masque_dates_invalides, 'date_naissance'] = df.loc[masque_dates_invalides, 'date_naissance'].apply(
                generer_date_deces_viable
            )

            # Suppression de la colonne intermédiaire
            df.drop(columns=['date_nais_dt'], inplace=True)

            #
            df.to_parquet(sfic_parquet, index=False)


        elif args.before == "CHECK_DATES":

            print(f"{index_before} / Traitement de la coherence des dates")
            df = pd.read_parquet(sfic_parquet)

            # Conversion en datetime
            df['date_naissance'] = pd.to_datetime(df['date_naissance'])
            df['date_deces'] = pd.to_datetime(df['date_deces'])

            # Détecter les incohérences
            condition = df['date_deces'] < df['date_naissance']

            # Intervertir les valeurs pour les lignes concernées
            temp = df.loc[condition, 'date_naissance'].copy()
            df.loc[condition, 'date_naissance'] = df.loc[condition, 'date_deces']
            df.loc[condition, 'date_deces'] = temp

            # Format des 2colonnes dates en YYYYMMDD
            df['date_naissance'] = df['date_naissance'].dt.strftime('%Y%m%d')
            df['date_deces'] = df['date_deces'].dt.strftime('%Y%m%d')

            #
            df.to_parquet(sfic_parquet, index=False)


        elif args.before == "PAYS_NAISSANCE":

            print(f"{index_before} / Traitement des pays de naissance")

            df=pd.read_parquet(sfic_parquet)

            nRecordsPaysAvant = compter_valeurs_uniques(df, "pays_naissance")

            # DATE NAISSANCE
            # changement sur les pays de naissance qui ont les parenthèses, on les supprime
            masque_pays_invalide = df["pays_naissance"].astype(str).str.startswith("(") & df["pays_naissance"].astype(str).str.endswith(")")
            df.loc[masque_pays_invalide, "pays_naissance"] = df.loc[masque_pays_invalide, "pays_naissance"].apply(generer_pays_viable)

            #
            masque_pays_invalide = df["pays_naissance"].astype(str).str.contains(
                "CENTRAFRICAINE|VIET|CALEDONIE|ITALIE|METROPOLITAINE|REUNION", case=False, na=False)
            df.loc[masque_pays_invalide, "pays_naissance"] = df.loc[masque_pays_invalide, "pays_naissance"].apply(
                generer_pays_viable)

            #
            masque_pays_invalide = df["pays_naissance"].astype(str).str.startswith(("E ", "(", "'", "AU ", "EN ", " "))
            df.loc[masque_pays_invalide, "pays_naissance"] = df.loc[masque_pays_invalide, "pays_naissance"].apply(
                generer_pays_viable)

            # Traitement des departements d'outrement dont le code est à tort dans pays_naissance
            # Extraire les 3 premiers caractères de code_commune
            # On définit les conditions (logique vectorisée)
            # Dictionnaire des correspondances entre codes INSEE et pays (en majuscules)
            correspondance_pays = {
                "971": "Guadeloupe",
                "972": "Martinique",
                "973": "Guyane",
                "974": "La Réunion",
                "975": "Saint-Pierre-et-Miquelon",
                "976": "Mayotte",
                "977": "Saint-Barthélemy",
                "978": "Saint-Martin",
                "985": "Mayotte",
                "986": "Wallis-et-Futuna",
                "987": "Polynésie Française",
                "988": "Nouvelle-Calédonie",
                "99101": "DANEMARK",
                "99109": "ALLEMAGNE",
                "99121": "SERBIE",
                "99122": "POLOGNE",
                "99127": "ITALIE",
                "99131": "BELGIQUE",
                "99134": "ESPAGNE",
                "99138": "PRINCIPAUTE DE MONACO",
                "99139": "PORTUGAL",
                "99140": "SUISSE",
                "99208": "TURQUIE",
                "99439": "SAINTE-LUCIE",
                "99351": "TUNISIE",
                "99352": "ALGERIE",
                "99350": "MAROC",
                "99341": "SENEGAL",
            }

            # "986": "Wallis-et-Futuna",
            # "987": "Polynésie Française",
            # "988": "Nouvelle-Calédonie"

            # Utilisation de np.select pour appliquer les correspondances
            conditions = [df["code_insee_naissance"].str.startswith(code) for code in correspondance_pays.keys()]
            choix = [correspondance_pays[code].upper() for code in correspondance_pays.keys()]  # Mettre en majuscules ici

            # Appliquer la correspondance à la colonne 'pays_naissance'
            df["pays_naissance"] = np.select(conditions, choix, default=df["pays_naissance"])

            #
            df.to_parquet(sfic_parquet, index=False)

            nRecordsPaysApres = compter_valeurs_uniques(df, "pays_naissance")

            # Création d'une liste triée et unique des pays
            # pays_uniques = df["pays_naissance"].dropna().unique()
            # pays_uniques = sorted(pays_uniques)
            #
            # # Conversion en DataFrame pour l'export CSV
            # df_pays = pd.DataFrame(pays_uniques, columns=["pays_naissance"])

            # Sélectionner uniquement les colonnes demandées
            colonnes_a_extraire = ['code_insee_naissance', 'commune_naissance', 'pays_naissance']

            # Extraire les lignes uniques
            df_uniques = df[colonnes_a_extraire].drop_duplicates()

            # Sauvegarde dans un fichier CSV
            #sLeFicCSV = "extract_pays_naissance.csv"
            # df_pays.to_csv(sFicPaysCSV, index=False, escapechar='\\')
            df_uniques.to_csv(sFicPaysCSV, index=False, escapechar='\\', sep=';')

            iResultat_pays=( nRecordsPaysApres / nRecordsPaysAvant) * 100
            print(f"\n@@ % des pays après traitement : {nRecordsPaysApres} / {nRecordsPaysAvant}  --> {iResultat_pays:.2f} %")
            config_yaml = set_yaml_value(config_yaml, f"{sDestNode}.pays_avant",f"{nRecordsPaysAvant}")
            config_yaml = set_yaml_value(config_yaml, f"{sDestNode}.pays_apres",f"{nRecordsPaysApres}")
            config_yaml = set_yaml_value(config_yaml, f"{sDestNode}.pays_ratio", f"{iResultat_pays:.2f} %")


        elif args.before == "VILLES_NAISSANCE":


            ###########################
            # Traitement des villes naissances
            ###########################
            print(f"{index_before} / Traitement des villes de naissance")

            df = pd.read_parquet(sfic_parquet)

            nRecordsVilleAvant = compter_valeurs_uniques(df, "commune_naissance")

            # Petit nettoyage
            #
            masque_ville_invalide = df["commune_naissance"].astype(str).str.startswith(("A ", "-", ".","*"))
            df.loc[masque_ville_invalide, "commune_naissance"] = df.loc[masque_ville_invalide, "commune_naissance"].apply(
                nettoyer_ville)

            # grand nettoyage
            df = nettoyer_colonne_villes_rapide(df, "commune_naissance", "pays_naissance", villes_a_corriger)

            nRecordsVilleApres = compter_valeurs_uniques(df, "commune_naissance")

            #
            df.to_parquet(sfic_parquet, index=False)

            # Création d'une liste triée et unique des villes
            #city_uniques = df["commune_naissance"].dropna().unique()
            #city_uniques = sorted(city_uniques)

            # Conversion en DataFrame pour l'export CSV
            #df_city = pd.DataFrame(city_uniques, columns=["commune_naissance"])

            # Sauvegarde dans un fichier CSV
            #df_city.to_csv("extract_city_naissance.csv", index=False, escapechar='\\')

            # Compter le nombre d'enregistrements dans le dataset filtré
            #sPaysNode = f"{sDestNode}"
            #nb_enregistrements_pays = df_pays.shape[0]  # shape[0] donne le nombre de lignes


            #sCityNode = f"traitement.city.nbre_enreg.{iHORO}"
            #nb_enregistrements_city = df_city.shape[0]  # shape[0] donne le nombre de lignes
            iResultat_cities = (nRecordsVilleApres / nRecordsVilleAvant) * 100
            print(f"\n@@ % des villes après traitement : {nRecordsVilleApres} / {nRecordsVilleAvant} --> {iResultat_cities:.2f} %")
            config_yaml = set_yaml_value(config_yaml, f"{sDestNode}.ville_avant", f"{nRecordsVilleAvant}")
            config_yaml = set_yaml_value(config_yaml, f"{sDestNode}.ville_apres", f"{nRecordsVilleApres}")
            config_yaml = set_yaml_value(config_yaml, f"{sDestNode}.ville_ratio", f"{iResultat_cities:.2f} %")


        elif args.before == "PRENOMS_0":


            ###########################
            # Traitement diverses valeurs
            ###########################
            print(f"{index_before} / Traitement des prenoms vides")

            df = pd.read_parquet(sfic_parquet)

            # Traitement des valeurs diverses
            #for id_valeur, (colonne, nouvelle_valeur) in modifications_diverses.items():
            #    df = modifier_valeur_par_id(df, id_valeur, colonne, nouvelle_valeur)

            # load fichier pickle des modifications diverses
            with open(sFicPicklePrenoms, 'rb') as f:
                modifications_diverses = pickle.load(f)

            # Appliquer les modifications
            for code_hash, champs in modifications_diverses.items():
                #print(f"Code hash : {code_hash}")
                #print(f"Champs : {champs}")
                if code_hash in df['ID_HASH'].values:
                    #print(f"Code hash trouvé : {code_hash}")
                    for col, val in champs.items():
                        df.loc[df['ID_HASH'] == code_hash, col] = val
                else:
                    print(f"[!] Code hash non trouvé : {code_hash}")

            #Suppression des tirets
            # df['nom'] = df['nom'].str.replace(r'^-', '', regex=True)

            #
            df.to_parquet(sfic_parquet, index=False)



        # Arrêter le timer et calculer le temps écoulé
        end_time = time.time()
        elapsed_time = f"{(end_time - start_time)/60:.2f}"  # end_time - start_time

        # Compter le nombre d'enregistrements dans le dataset filtré
        nb_enregistrements = compter_lignes_parquet(sfic_parquet)
        print(
            f"\n@@ Nbre d'enreg dans le dataset  : {nb_enregistrements} et temps d'exec : {elapsed_time} minutes")
        config_yaml = set_yaml_value(config_yaml, f"{sDestNode}.nbre_enreg", nb_enregistrements)
        config_yaml = set_yaml_value(config_yaml, f"{sDestNode}.temps_exec", elapsed_time)

        # Sauvegarde si fichier parquet OK techniquement
        bRet = verifier_validite_parquet(sfic_parquet)
        if bRet:
            print(f"Traitement sur le fichier parquet OK")
            copier_fichier(sfic_parquet, sfic_save)
            copier_fichier(sfic_parquet, sFic_save_etape)

        else:
            print(f"#### Traitement sur le fichier parquet KO ####")
            exit(1)

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

            # Filtrer les lignes où `date_deces` commence par iAnne
            #df_filtered = df[df['date_deces'].astype(str).str.startswith(iAnnee)]

        else:
            #print("Charger du gros fichier parquet")
            # Charger seulement les colonnes nécessaires avec pyarrow
            df = pd.read_parquet(sfic_parquet)

            # supprimer les colonnes
            # Exclure certaines colonnes
            #df = df.drop(columns=["numero_acte_deces", "fichier_origine","opposition"])

            # Filtrer les lignes où `date_naissance` commence par iAnne
            if int(iAnnee) == 1970:
                #print("Traitement sur l'annee 1970 et avant")
                df_filtered = df[df['date_deces'].astype(str).str[:4].astype(int) <= 1970]
            else:
                #print(f"Traitement sur l'annee {iAnnee}")
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

    elif args.action == "PREPARE_PRENOMS":

        print(f"Action : {args.action} pour l'année {iAnnee}")

        # Démarrer le timer
        start_time = time.time()

        # Lecture des parametres Parquet du fichier Yaml
        try:
            # Parquet
            parquet_path = get_yaml_value(config_yaml, "main.parquet.parquet_path", required=True)
            pickle_path = get_yaml_value(config_yaml, "main.pickle.pickle_path", required=True)
            pickle_prenoms = get_yaml_value(config_yaml, "main.pickle.pickle_prenoms", required=True)

            # Section Annee
            #fichier_annee_date = get_yaml_value(config_yaml, iAnnee, "date")
        except MissingKeyError as e:
            print(e)
            exit(1)


        # Lire le fichier Parquet annee des personnes
        sfic_parquet_annee = f"{parquet_path}/deces_{iAnnee}.parquet"

        # Fichiers Pickle des modifications diverses
        sFicPicklePrenoms = f"{pickle_path}/{pickle_prenoms}"

        if not os.path.exists(sfic_parquet_annee):
            print(f"Le fichier {sfic_parquet_annee} n'existe pas")
            print("Il faut d'abord faire PREPARE_DIR")
            exit(1)

        # Démarrer le timer
        end_time = time.time()

        # Chargement du fichier parquet
        df = pd.read_parquet(sfic_parquet_annee)

        # Filtrage : prenoms est vide ou null (None ou NaN)
        df_prenoms = df[df['prenoms'].isna() | (df['prenoms'].astype(str).str.strip() == '')]

        #print(df_prenoms.head())

        # Transformation des prenoms
        dict_prenoms = detect_prenoms(df_prenoms, prenoms_FlashText)

        # Sauvegarde du dictionnaire
        sFicPicklePrenomsAnnee = f"prenoms_{iAnnee}"
        save_pickle(dict_prenoms, pickle_path, sFicPicklePrenomsAnnee)


        # Calcul du temps écoulé
        elapsed_time = end_time - start_time

        # Compter le nombre d'enregistrements dans le dataset filtré
        print(f"\n@@ Pour l'annee {iAnnee}, temps d'exec : {elapsed_time:.2f} secondes")


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


        # csv to parquet
        csv_to_parquet(sPathOUTPers + '/CONCAT_PERSONNES_' + iAnnee + '.csv', sPathOUTPers + '/CONCAT_PERSONNES_' + iAnnee + '.parquet')

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


        # csv to parquet
        csv_to_parquet(sPathOUTPren + '/CONCAT_PRENOMS_' + iAnnee + '.csv', sPathOUTPren + '/CONCAT_PRENOMS_' + iAnnee + '.parquet')

        # Démarrer le timer
        end_time = time.time()

        elapsed_time = end_time - start_time

        # Compter le nombre d'enregistrements dans le dataset filtré
        print(f"\n@@ Pour l'annee {iAnnee}, temps d'exec : {elapsed_time:.2f} secondes")


    elif args.action == "LOAD":

        print(f"Action : {args.action} pour l'année {iAnnee}")

        # Lecture des parametres SGBD du fichier Yaml
        try:
            # Parquet
            sgbd_path = get_yaml_value(config_yaml, "main.sgbd.db_path", required=True)
            sgpd_name = get_yaml_value(config_yaml, "main.sgbd.db_name", required=True)
            sgbd_timeout = get_yaml_value(config_yaml, "main.sgbd.timeoutsqlite", required=True)

        except MissingKeyError as e:
            print(e)
            exit(1)

        fFilePersonneConcat=f"{sPathOUTPers}/CONCAT_PERSONNES_{iAnnee}.csv"
        fFilePrenomConcat=f"{sPathOUTPren}/CONCAT_PRENOMS_{iAnnee}.csv"

        if not os.path.exists(fFilePersonneConcat) or not os.path.exists(fFilePrenomConcat):
            print(f"Test du repertoire {fFilePersonneConcat} ou {fFilePrenomConcat} : KO")
            print("Il faut d'abord faire PREPARE_DATAS")
            exit(1)

        # suppresions des enregistrements existant
        # del_records_by_year(iAnnee)

        # Suppression de la sgbd deces_xxxx.db si existe et copie depuis la base de données source
        sgbd_annee=f"deces_{iAnnee}.db"
        if os.path.exists(f"{sgbd_path}/{sgbd_annee}"):
            os.remove(f"{sgbd_path}/{sgbd_annee}")
        copier_fichier(f"{sgbd_path}/{sgpd_name}", f"{sgbd_path}/{sgbd_annee}")

        # creation de la connexion à la base de données SQLite
        sDBPathAnnee = os.path.join(os.path.abspath(os.path.join(sgbd_path, sgbd_annee)))

        # Créer l'engine SQLite avec des arguments de connexion spécifiques
        if os.path.exists(sDBPathAnnee):
            engineAnnee = create_engine(
                'sqlite:///' + sDBPathAnnee,
                connect_args={'timeout': sgbd_timeout, 'check_same_thread': False}
            )
        else:
            print(f"\n### Le fichier '{sDBPathAnnee}' n'existe pas\n")
            exit(1)

        # Activer le mode Write-Ahead Logging (WAL)
        with engineAnnee.connect() as connectionAnnee:
            wal_modeAnnee = connectionAnnee.execute(text("PRAGMA journal_mode=WAL")).scalar()
            if wal_modeAnnee == "wal":
                print("Mode WAL activé avec succès.")
            else:
                print("Échec de l'activation du mode WAL.")

        # Création de la session
        SessionsAnnee = sessionmaker(bind=engineAnnee)

        Sess_Annee = SessionsAnnee()

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

        # fonction currifaction
        inject_with_session_factory = partial(inject_csv_to_Personne_threadsafe, session=SessionsAnnee)

        with ThreadPoolExecutor(max_workers=max_files_in_parallel) as executor:
            #inject_with_session = partial(inject_csv_to_Personne, SessionsAnnee())
            executor.map(inject_with_session_factory, list_files_personnes)

        print("Injection des personnes terminée")

        print(f"\n*** Nombre d'enreg total des personnes : {iTotal} pour l'annee {iAnnee} dans {len(list_files_personnes)} fichiers")
        nbre_enreg_table = Sess_Annee.query(func.count(Personne.dd_date)).filter(func.substr(Personne.dd_date, 1, 4) == iAnnee).scalar()
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

        # Utilisation de ProcessPoolExecutor pour charger les fichiers 'personne' en parallele
        # fonction currifaction
        inject_with_session_factory = partial(inject_csv_to_Prenoms_threadsafe, session=SessionsAnnee)

        with ThreadPoolExecutor(max_workers=max_files_in_parallel) as executor:
            executor.map(inject_with_session_factory, list_files_prenoms, )

        print("Injection des prenoms terminée")

        print(f"\n*** Nombre d'enreg total des prenoms : {iTotal} pour l'annee {iAnnee} dans {len(list_files_prenoms)} fichiers")
        nbre_enreg_table = Sess_Annee.query(func.count(Prenoms.fichier)).filter(Prenoms.fichier == iAnnee).scalar()
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

    elif args.action == "RAPPORT":

        print(f"Action : {args.action} pour l'année {iAnnee}")


        try:
            # Parquet
            rapport_path = get_yaml_value(config_yaml, "main.rapport.rapport_path", required=True)
        except MissingKeyError as e:
            print(e)
            exit(1)

        # Démarrer le timer
        start_time = time.time()

        #Lecture du fichier parquet selon l'annee
        #df = pd.read_csv('../DATAS/1970/OUT/PERSONNES/CONCAT_PERSONNES_1970.cs', sep=';', header=0, low_memory=False)
        df = pd.read_parquet(f'{sPathOUTPers}/CONCAT_PERSONNES_{iAnnee}.parquet')

        # Rapport
        profile = ProfileReport(df)
        profile.to_file(rapport_path + "/1970_rapport_personnes.html")

        # Démarrer le timer
        end_time = time.time()

        elapsed_time = end_time - start_time

        # Compter le nombre d'enregistrements dans le dataset filtré
        print(f"\n@@ Pour l'annee {iAnnee}, temps d'exec : {elapsed_time:.2f} secondes")

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
    parser.add_argument("--years", dest="years", nargs="+", type=str, required=False, action="store",
                        help="Year(s) to perform")
    parser.add_argument("--action", dest="action", type=str, choices=PERFORM.keys(), required=True,
                        help="Action to perform")
    parser.add_argument('--before', choices=BEFORE.keys(), action='store', dest='before', required=False,
                        help="Type of data cleaning to perform before treatment")
    args = parser.parse_args()


    # lecture parametres pour le parallelisme
    try:
        max_threads_annee = get_yaml_value(config_yaml, "main.threads.max_threads_annee", required=True)
        max_files_in_parallel_one_year = get_yaml_value(config_yaml, "main.threads.max_files_in_parallel_one_year", required=True)
        max_files_in_parallel_many_year = get_yaml_value(config_yaml, "main.threads.max_files_in_parallel_many_year", required=True)
    except MissingKeyError as e:
        print(e)
        exit(1)

    # Validation croisée
    if args.action == 'PREPARE_PARQUET' and not args.before:
        parser.error("--before est obligatoire quand --action PREPARE_PARQUET est spécifié.")
    elif args.action == 'PREPARE_PARQUET':
        years=["1"]
    elif args.action != 'PREPARE_PARQUET' and not args.years:
        parser.error("--years est obligatoire quand --action n'est pas PREPARE_PARQUET.")
    else:
        # Obtenir la liste des années à traiter
        years = parse_years(args.years)
        max_files_in_parallel = max_files_in_parallel_many_year

        if len(years) == 1:
            max_files_in_parallel = max_files_in_parallel_one_year

        print(f"\nNombre d'années à traiter : {len(years)}, \n\tmax_threads_annee : {max_threads_annee}, \n\tmax_files_in_parallel : {max_files_in_parallel}")

    #


    if args.action == "PREPARE_DATAS":

        # Chargement du dictionnaire des coordonnées geo france
        print("Chargement du dictionnaire des coordonnées geo france")
        dict_coord_france=charger_insee_geo(Sess_deces)

        # Chargement du dictionnaire des coordonnées geo foreign
        print("Chargement du dictionnaire des coordonnées geo foreign")
        dict_coord_foreign=charger_countries_geo(Sess_deces)


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
