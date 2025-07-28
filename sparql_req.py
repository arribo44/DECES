import requests
import csv
import sys

def construire_requete_sparql(annee):
    requete = f"""
    SELECT ?person ?personLabel ?givenNameLabel ?familyNameLabel 
           ?birthDate ?birthPlaceLabel ?birthCountryLabel 
           ?deathDate ?deathPlaceLabel ?deathCountryLabel 
           ?occupationLabel
    WHERE {{
      ?person wdt:P31 wd:Q5.          # instance de personne
      ?person wdt:P570 ?deathDate.    # date de décès
      FILTER(YEAR(?deathDate) = {annee})
      ?person wdt:P20 ?deathPlace.    # lieu de décès
      ?deathPlace wdt:P17 wd:Q142.    # pays du lieu de décès = France

      OPTIONAL {{ ?person wdt:P569 ?birthDate. }}
      OPTIONAL {{ ?person wdt:P19 ?birthPlace. }}
      OPTIONAL {{ ?birthPlace wdt:P17 ?birthCountry. }}
      OPTIONAL {{ ?deathPlace wdt:P17 ?deathCountry. }}
      OPTIONAL {{ ?person wdt:P735 ?givenName. }}
      OPTIONAL {{ ?person wdt:P734 ?familyName. }}
      OPTIONAL {{ ?person wdt:P106 ?occupation. }}

      SERVICE wikibase:label {{
        bd:serviceParam wikibase:language "fr,en".
      }}
    }}
    ORDER BY ?deathDate
    """
    return requete

def executer_requete_sparql(requete):
    url = "https://query.wikidata.org/sparql"
    headers = {"Accept": "application/sparql-results+json"}
    response = requests.get(url, params={"query": requete}, headers=headers)
    response.raise_for_status()
    return response.json()

def exporter_csv(resultats, nom_fichier):
    champs = [
        "personLabel", "givenNameLabel", "familyNameLabel", "occupationLabel",
        "birthDate", "birthPlaceLabel", "birthCountryLabel",
        "deathDate", "deathPlaceLabel", "deathCountryLabel"
    ]
    with open(nom_fichier, mode="w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=champs, delimiter=";")
        writer.writeheader()
        for item in resultats["results"]["bindings"]:
            ligne = {}
            for champ in champs:
                ligne[champ] = item.get(champ, {}).get("value", "")
            writer.writerow(ligne)

def main():
    if len(sys.argv) != 2:
        print("Usage : python extract_deaths_france.py <année>")
        sys.exit(1)

    annee = sys.argv[1]
    requete = construire_requete_sparql(annee)
    print(f"Requête en cours pour l’année {annee}...")
    resultats = executer_requete_sparql(requete)
    nom_fichier = f"deces_france_{annee}.csv"
    exporter_csv(resultats, nom_fichier)
    print(f"Fichier CSV généré : {nom_fichier}")

if __name__ == "__main__":
    main()

