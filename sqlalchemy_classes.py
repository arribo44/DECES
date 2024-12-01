
# models.py
#
from sqlalchemy import Column, Integer, String, Sequence, Float
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.exc import OperationalError

Base = declarative_base()

# Définition de la classe Personne pour mapper la table
class Personne(Base):
    __tablename__ = 'personnes'

    id = Column(Integer, Sequence('personnes_id_seq'), primary_key=True, autoincrement=True)
    nom = Column(String)
    prenom = Column(String)
    sexe = Column(Integer)
    age = Column(Integer)
    long_nom = Column(Integer)
    nbre_prenoms = Column(Integer)
    db_an = Column(Integer)
    db_mois = Column(Integer)
    db_jour = Column(Integer)
    db_lib_jour = Column(String)
    db_week = Column(Integer)
    db_code_commune = Column(String)
    db_lib_commune = Column(String)
    db_code_dpt = Column(Integer)
    db_pays = Column(String)
    db_continent = Column(String)
    dd_an = Column(Integer)
    dd_mois = Column(Integer)
    dd_jour = Column(Integer)
    dd_lib_jour = Column(String)
    dd_week = Column(Integer)
    dd_code_commune = Column(String)
    dd_lib_commune = Column(String)
    dd_code_dpt = Column(Integer)
    dd_pays = Column(String)
    dd_continent = Column(String)
    iso_date = Column(Integer)
    iso_dpt = Column(Integer)
    iso_commune = Column(Integer)
    iso_pays = Column(Integer)
    distance = Column(Integer)


# Définition de la classe Prenoms pour mapper la table
class Prenoms(Base):
    __tablename__ = 'prenoms'

    id = Column(Integer, Sequence('prenoms_id_seq'), primary_key=True, autoincrement=True)
    evt = Column(String)
    level = Column(String)
    sexe = Column(Integer)
    annee = Column(Integer)
    mois = Column(Integer)
    jour = Column(Integer)
    code_dpt = Column(Integer)
    prenom = Column(String)
    fichier = Column(Integer)


# Définition de la classe localisation FRANCE pour mapper la table
class FRANCE_LOC(Base):
    __tablename__ = 'FRANCE_LOC'

    Code_INSEE = Column(String, primary_key=True)
    Code_Old_INSEE = Column(String)
    Code_Postal = Column(String)
    Commune = Column(String)
    Departement = Column(String)
    Region = Column(String)
    Statut = Column(String)
    Altitude_Moyenne = Column(Float)
    Superficie = Column(Float)
    Population = Column(Float)
    geo_point_2d = Column(String)

class UNKNOWN_LOC(Base):
    __tablename__ = 'UNKNOWN_LOC'

    Code_INSEE = Column(String, primary_key=True)
    Commune = Column(String)
    Pays = Column(String)
    Fichier = Column(String)


# Définition de la classe localisation FRANCE pour mapper la table
class COUNTRIES_LOC(Base):
    __tablename__ = 'COUNTRIES_LOC'

    Code_INSEE = Column(String, primary_key=True)
    Country = Column(String)
    Capital = Column(String)
    geo_point_2d = Column(String)
    Continent = Column(String)

def retry_commit(session, retries=5, delay=1):
    for attempt in range(retries):
        try:
            session.commit()
            return
        except OperationalError as e:
            if "database is locked" in str(e):
                print(f"Retry {attempt + 1}/{retries}: Database is locked. Retrying in {delay}s...")
                time.sleep(delay)
            else:
                raise
    raise OperationalError("Database remained locked after retries.")