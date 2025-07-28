import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
import plotly.express as px
import json
import requests

import random

# Charger les données GeoJSON des départements français
geojson_url = 'https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/departements-version-simplifiee.geojson'
geojson_data = requests.get(geojson_url).json()


# Liste des départements français avec leurs codes INSEE
departements = [
    {'nom': 'Ain', 'code': '01'}, {'nom': 'Aisne', 'code': '02'}, {'nom': 'Allier', 'code': '03'},
    {'nom': 'Alpes-de-Haute-Provence', 'code': '04'}, {'nom': 'Hautes-Alpes', 'code': '05'},
    {'nom': 'Alpes-Maritimes', 'code': '06'}, {'nom': 'Ardèche', 'code': '07'}, {'nom': 'Ardennes', 'code': '08'},
    {'nom': 'Ariège', 'code': '09'}, {'nom': 'Aube', 'code': '10'}, {'nom': 'Aude', 'code': '11'},
    {'nom': 'Aveyron', 'code': '12'}, {'nom': 'Bouches-du-Rhône', 'code': '13'}, {'nom': 'Calvados', 'code': '14'},
    {'nom': 'Cantal', 'code': '15'}, {'nom': 'Charente', 'code': '16'}, {'nom': 'Charente-Maritime', 'code': '17'},
    {'nom': 'Cher', 'code': '18'}, {'nom': 'Corrèze', 'code': '19'}, {'nom': 'Corse-du-Sud', 'code': '2A'},
    {'nom': 'Haute-Corse', 'code': '2B'}, {'nom': 'Côte-d\'Or', 'code': '21'}, {'nom': 'Côtes-d\'Armor', 'code': '22'},
    {'nom': 'Creuse', 'code': '23'}, {'nom': 'Dordogne', 'code': '24'}, {'nom': 'Doubs', 'code': '25'},
    {'nom': 'Drôme', 'code': '26'}, {'nom': 'Eure', 'code': '27'}, {'nom': 'Eure-et-Loir', 'code': '28'},
    {'nom': 'Finistère', 'code': '29'}, {'nom': 'Gard', 'code': '30'}, {'nom': 'Haute-Garonne', 'code': '31'},
    {'nom': 'Gers', 'code': '32'}, {'nom': 'Gironde', 'code': '33'}, {'nom': 'Hérault', 'code': '34'},
    {'nom': 'Ille-et-Vilaine', 'code': '35'}, {'nom': 'Indre', 'code': '36'}, {'nom': 'Indre-et-Loire', 'code': '37'},
    {'nom': 'Isère', 'code': '38'}, {'nom': 'Jura', 'code': '39'}, {'nom': 'Landes', 'code': '40'},
    {'nom': 'Loir-et-Cher', 'code': '41'}, {'nom': 'Loire', 'code': '42'}, {'nom': 'Haute-Loire', 'code': '43'},
    {'nom': 'Loire-Atlantique', 'code': '44'}, {'nom': 'Loiret', 'code': '45'}, {'nom': 'Lot', 'code': '46'},
    {'nom': 'Lot-et-Garonne', 'code': '47'}, {'nom': 'Lozère', 'code': '48'}, {'nom': 'Maine-et-Loire', 'code': '49'},
    {'nom': 'Manche', 'code': '50'}, {'nom': 'Marne', 'code': '51'}, {'nom': 'Haute-Marne', 'code': '52'},
    {'nom': 'Mayenne', 'code': '53'}, {'nom': 'Meurthe-et-Moselle', 'code': '54'}, {'nom': 'Meuse', 'code': '55'},
    {'nom': 'Morbihan', 'code': '56'}, {'nom': 'Moselle', 'code': '57'}, {'nom': 'Nièvre', 'code': '58'},
    {'nom': 'Nord', 'code': '59'}, {'nom': 'Oise', 'code': '60'}, {'nom': 'Orne', 'code': '61'},
    {'nom': 'Pas-de-Calais', 'code': '62'}, {'nom': 'Puy-de-Dôme', 'code': '63'}, {'nom': 'Pyrénées-Atlantiques', 'code': '64'},
    {'nom': 'Hautes-Pyrénées', 'code': '65'}, {'nom': 'Pyrénées-Orientales', 'code': '66'}, {'nom': 'Bas-Rhin', 'code': '67'},
    {'nom': 'Haut-Rhin', 'code': '68'}, {'nom': 'Rhône', 'code': '69'}, {'nom': 'Haute-Saône', 'code': '70'},
    {'nom': 'Saône-et-Loire', 'code': '71'}, {'nom': 'Sarthe', 'code': '72'}, {'nom': 'Savoie', 'code': '73'},
    {'nom': 'Haute-Savoie', 'code': '74'}, {'nom': 'Paris', 'code': '75'}, {'nom': 'Seine-Maritime', 'code': '76'},
    {'nom': 'Seine-et-Marne', 'code': '77'}, {'nom': 'Yvelines', 'code': '78'}, {'nom': 'Deux-Sèvres', 'code': '79'},
    {'nom': 'Somme', 'code': '80'}, {'nom': 'Tarn', 'code': '81'}, {'nom': 'Tarn-et-Garonne', 'code': '82'},
    {'nom': 'Var', 'code': '83'}, {'nom': 'Vaucluse', 'code': '84'}, {'nom': 'Vendée', 'code': '85'},
    {'nom': 'Vienne', 'code': '86'}, {'nom': 'Haute-Vienne', 'code': '87'}, {'nom': 'Vosges', 'code': '88'},
    {'nom': 'Yonne', 'code': '89'}, {'nom': 'Territoire de Belfort', 'code': '90'}, {'nom': 'Essonne', 'code': '91'},
    {'nom': 'Hauts-de-Seine', 'code': '92'}, {'nom': 'Seine-Saint-Denis', 'code': '93'}, {'nom': 'Val-de-Marne', 'code': '94'},
    {'nom': 'Val-d\'Oise', 'code': '95'}
]

# Générer un dataset de naissances par année pour chaque département
annees = list(range(1970, 1981))
data = []

for annee in annees:
    for dept in departements:
        data.append({
            'annee': annee,
            'departement': dept['code'],
            'nom_departement': dept['nom'],
            'naissances': random.randint(500, 5000)  # Génère un nombre aléatoire de naissances
        })

#print(data)
# Convertir en DataFrame Pandas
df = pd.DataFrame(data)

print(df)
print(df.dtypes)


# Initialisation de l'application Dash
app = dash.Dash(__name__)

# Layout de l'application
app.layout = html.Div([

    # Carte
    html.H1("Carte Choropleth des Naissances par Département"),

    # Dropdown pour sélectionner l'année
    dcc.Dropdown(
        id='annee-dropdown',
        options=[{'label': str(year), 'value': year} for year in df['annee'].unique()],
        value=df['annee'].min(),
        clearable=False,
    ),

    # Carte choropleth
    dcc.Graph(id='choropleth-map')
])


# Callback pour mettre à jour la carte choropleth
@app.callback(
    Output('choropleth-map', 'figure'),
    [Input('annee-dropdown', 'value')]
)
def update_map(selected_year):
    # Filtrer les données par l'année sélectionnée
    filtered_df = df[df['annee'] == selected_year]

    # Créer la carte choropleth
    fig = px.choropleth(
        filtered_df,
        geojson=geojson_data,
        locations='departement',
        featureidkey='properties.code',  # Associe les départements du GeoJSON avec ceux du DataFrame
        color='naissances',
        color_continuous_scale="Viridis",  # Choix de la palette de couleurs
        labels={'naissances': 'Nombre de naissances'},
        hover_name='departement',
        title=f'Nombre de Naissances en {selected_year} par Département'
    )

    # Mise à jour des contours et des limites géographiques
    fig.update_geos(
        visible=False,  # Cache les axes géographiques
        fitbounds="locations",  # Zoom sur la France métropolitaine en fonction des données
        showcountries=False,  # Affiche les frontières des pays
        countrycolor="black",  # Couleur des frontières des pays
        showsubunits=True,  # Affiche les frontières des sous-unités (départements)
        subunitcolor="black",  # Couleur des frontières des départements
    )

    # Ajouter des contours noirs autour des départements
    fig.update_traces(marker_line_width=1, marker_line_color='black')

    return fig


# Exécution de l'application Dash
if __name__ == '__main__':
    app.run_server(debug=True)
