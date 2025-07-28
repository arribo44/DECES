#
#
#

import sqlite3
import pandas as pd
import os
import plotly.graph_objects as go

from dash import Dash, dcc, html, Input, Output
import dash_bootstrap_components as dbc

import json
import requests

# Charger les données GeoJSON des départements français
geojson_url = 'https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/departements-version-simplifiee.geojson'
geojson_data = requests.get(geojson_url).json()

# Construire le chemin absolu
db_path = os.path.abspath('../DATAS/DB/deces_sequential.db')

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

# Fonction pour extraire les données naissance par année
def get_birth_by_year(year, laconn):

    # Connexion à la base de données SQLite
    #conn = sqlite3.connect(db_path)

    #
    query = f"""
    SELECT db_mois, sexe, COUNT(*) as Nbre
    FROM personnes
    where db_an='{year}'
    and dd_an='{year}'
    GROUP BY db_mois, sexe
    ORDER BY db_mois
    """

    df = pd.read_sql_query(query, laconn)

    # Calculer le total de naissances par mois
    totals = df.groupby('db_mois')['Nbre'].sum().reset_index()

    # Ajouter une colonne 'sexe' pour marquer les lignes de total
    totals['sexe'] = 'Total'

    # Fusionner les totaux avec le DataFrame original
    df = pd.concat([df, totals[['db_mois', 'sexe', 'Nbre']]], ignore_index=True)

    # Trier le DataFrame par 'db_mois' pour maintenir l'ordre
    df = df.sort_values(by=['db_mois', 'sexe'], ascending=[True, False]).reset_index(drop=True)

    # Remplacement des valeurs numériques par des labels
    df['sexe'] = df['sexe'].replace({1: 'M', 2: 'F'})

    #print(df)

    return df

# Fonction pour extraire les données sexe et age par année de deces
def get_age_by_year_sexe(year_death, laconn):

    #
    query = f"""
    SELECT sexe, age
    FROM personnes
    where dd_an='{year_death}'
    """

    df = pd.read_sql_query(query, laconn)

    # Remplacement des valeurs numériques par des labels
    df['sexe'] = df['sexe'].replace({1: 'M', 2: 'F'})

    #print(df)

    return df

def get_birth_by_year_dept(year, sexe, laconn):

    # Connexion à la base de données SQLite
    #conn = sqlite3.connect(db_path)

    # print("ICI")
    #print(f"fonction : {sexe}")
    clauses=f"""where db_an='{year}' and dd_an='{year}'"""
    if sexe=='M':
        clauses=clauses + ' and sexe=1 '
    elif sexe=='F':
        clauses = clauses + ' and sexe=2 '

    query = f"""
    SELECT db_code_dpt, COUNT(*) as Nbre
    FROM personnes 
    {clauses} 
    GROUP BY db_code_dpt
    ORDER BY db_code_dpt
    """
    #print(query)
    df = pd.read_sql_query(query, laconn)

    data = []
    i=0
    #data.append({'annee': '1970', 'code': '44', 'nom_departement': 'Loire-Atlantique', 'naissance': 44})
    #print(df[df['db_code_dpt'] == 95]['Nbre'].values.item())

    #print(94 in df['db_code_dpt'])
    #print(95 in df['db_code_dpt'].values)

    for dept in departements:
        #

        if dept['code'] == '2A' or dept['code'] == '2B':
            if i == 0 :
                tmpDept=20
                i=1
        else:
            tmpDept=int(dept['code'])

        #print(f"tmpDept : {tmpDept}")
        if tmpDept in df['db_code_dpt'].values  :
            #print(f"OK : dept {tmpDept}")
            valTmp=df[df['db_code_dpt'] == tmpDept]['Nbre'].values.item()
            #print(f"OK : dept {tmpDept} / {valTmp}")
        else:
            #print(f"### KO : dept {tmpDept}")
            valTmp=0

        strDept=str(tmpDept)
        if tmpDept<10:
            strDept="0" + str(tmpDept)

        #strDept = lambda tmpDept: str(tmpDept).zfill(2)

        data.append({
            'annee': year,
            'departement': strDept,
            'nom_departement': dept['nom'],
            'naissances': valTmp

        })

    return data

def get_death_by_year_dept(year, sexe, laconn):

    # Connexion à la base de données SQLite
    #conn = sqlite3.connect(db_path)

    # print("ICI")
    #print(f"fonction : {sexe}")
    clauses=f"""where dd_an='{year}'"""
    if sexe=='M':
        clauses=clauses + ' and sexe=1 '
    elif sexe=='F':
        clauses = clauses + ' and sexe=2 '

    query = f"""
    SELECT dd_code_dpt, COUNT(*) as Nbre
    FROM personnes 
    {clauses} 
    GROUP BY dd_code_dpt
    ORDER BY dd_code_dpt
    """
    #print(query)
    df = pd.read_sql_query(query, laconn)

    data = []
    i=0
    #data.append({'annee': '1970', 'code': '44', 'nom_departement': 'Loire-Atlantique', 'naissance': 44})
    #print(df[df['db_code_dpt'] == 95]['Nbre'].values.item())

    #print(94 in df['db_code_dpt'])
    #print(95 in df['db_code_dpt'].values)

    for dept in departements:
        #

        if dept['code'] == '2A' or dept['code'] == '2B':
            if i == 0 :
                tmpDept=20
                i=1
        else:
            tmpDept=int(dept['code'])

        #print(f"tmpDept : {tmpDept}")
        if tmpDept in df['dd_code_dpt'].values  :
            #print(f"OK : dept {tmpDept}")
            valTmp=df[df['dd_code_dpt'] == tmpDept]['Nbre'].values.item()
            #print(f"OK : dept {tmpDept} / {valTmp}")
        else:
            #print(f"### KO : dept {tmpDept}")
            valTmp=0

        strDept=str(tmpDept)
        if tmpDept<10:
            strDept="0" + str(tmpDept)

        #strDept = lambda tmpDept: str(tmpDept).zfill(2)

        data.append({
            'annee': year,
            'departement': strDept,
            'nom_departement': dept['nom'],
            'deces': valTmp

        })

    return data


def get_death_by_year(year, laconn):

    # Connexion à la base de données SQLite
    conn = sqlite3.connect(db_path)

    query = f"""
    SELECT db_an, sexe, COUNT(*) as Nbre
    FROM personnes
    where dd_an='{year}'
    GROUP BY db_an, sexe
    ORDER BY db_an
    """
    df = pd.read_sql_query(query, laconn)

    # Calculer le total des deces par mois
    #totals = df.groupby('db_an')['Nbre'].sum().reset_index()

    # Ajouter une colonne 'sexe' pour marquer les lignes de total
    #totals['sexe'] = 'Total'

    # Fusionner les totaux avec le DataFrame original
    #df = pd.concat([df, totals[['db_an', 'sexe', 'Nbre']]], ignore_index=True)

    # Trier le DataFrame par 'dd_an' pour maintenir l'ordre
    #df = df.sort_values(by=['db_an', 'sexe'], ascending=[True, False]).reset_index(drop=True)

    # Remplacement des valeurs numériques par des labels
    df['sexe'] = df['sexe'].replace({1: 'M', 2: 'F'})

    return df

def get_death_by_month(year, laconn):

    # Connexion à la base de données SQLite
    conn = sqlite3.connect(db_path)

    # print("ICI")
    query = f"""
    SELECT dd_mois, sexe, COUNT(*) as Nbre
    FROM personnes
    where dd_an='{year}'
    GROUP BY dd_mois, sexe
    ORDER BY dd_mois
    """
    df = pd.read_sql_query(query, laconn)

    # print(df)

    # Calculer le total des deces par mois
    totals = df.groupby('dd_mois')['Nbre'].sum().reset_index()

    # Ajouter une colonne 'sexe' pour marquer les lignes de total
    totals['sexe'] = 'Total'

    # Fusionner les totaux avec le DataFrame original
    df = pd.concat([df, totals[['dd_mois', 'sexe', 'Nbre']]], ignore_index=True)

    # Trier le DataFrame par 'dd_mois' pour maintenir l'ordre
    df = df.sort_values(by=['dd_mois', 'sexe'], ascending=[True, False]).reset_index(drop=True)

    # Remplacement des valeurs numériques par des labels
    df['sexe'] = df['sexe'].replace({1: 'M', 2: 'F'})

    return df


def get_year(laconn):
    # print("ICI")
    query = f"""
    SELECT db_an
    FROM personnes
    where db_an >= 1970
    GROUP BY db_an
    ORDER BY db_an
    """
    df = pd.read_sql_query(query, laconn)
    return df

# Fonction pour calculer les indicateurs
def calculate_indicators(df):
    #total_enregistrements = df['total'].sum()  # Total des enregistrements
    hommes = df[df['sexe'] == 'M']['Nbre'].sum()  # Total des hommes
    femmes = df[df['sexe'] == 'F']['Nbre'].sum()  # Total des femmes
    total_enregistrements = hommes + femmes
    return total_enregistrements, hommes, femmes

# Connexion à la base de données SQLite
laconn = sqlite3.connect(db_path)

# Extraire les données pour l'année sélectionnée
data_year = get_year(laconn)

# print(df)
one_year = data_year.sample(n=1)

#
laconn.close()

# Définir les couleurs pour chaque sexe
# color_map = {1: 'blue', 2: 'pink'}
color_map = {'M': 'blue', 'F': 'orange', 'Total': 'red'}
color_map_choro = {
    'M': 'Blues',  # bleu clair pour 'info'
    'F': 'Oranges',  # gris pour 'secondary'
    'Total': 'Purples'  # bleu par défaut pour 'primary'
}

external_stylesheets = [
    {
        "href": (
            "https://fonts.googleapis.com/css2?"
            "family=Lato:wght@400;700&display=swap"
        ),
        "rel": "stylesheet",
    },
]

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.title = "Analyse naissances et décès en France !"

# Layout de l'application avec un dropdown pour l'année et un graphique
app.layout = dbc.Container(
    children=[
        # dbc.Row(
        #     dbc.Col(
        #         html.Div(
        #             children=[
        #                 html.P(children="🥑", className="header-emoji"),
        #                 html.H1(children="Stats sur la population française", className="header-title"),
        #                 html.P(children=("Statistiques des naissances et morts en France "
        #                                  "Données issues des actes de décès depuis 1970"),
        #                        className="header-description"),
        #             ],
        #         ),
        #         className="text-center"
        #     ),
        # ),
# Header with Navbar
    dbc.NavbarSimple(
        # children=[
        #     dbc.NavItem(dbc.NavLink("Home", href="/")),
        #     dbc.NavItem(dbc.NavLink("About", href="/about")),
        #     dbc.NavItem(dbc.NavLink("Contact", href="/contact")),
        # ],
        brand="Statistiques des naissances et morts en France par rapport à une année des décès (depuis 1970)",
        brand_href="#",
        color="primary",
        dark=True,
        className="mb-4"
    ),
        dbc.Row(
            dbc.Col(
                # Wrapper with custom style
                html.Div([
                    # Label and Dropdown in the same row
                    dbc.Row([
                        dbc.Col(html.Label("Année de décès", className="text-white"), width="auto"),
                        dbc.Col(
                            dcc.Dropdown(
                                id="year-dropdown",
                                options=[{'label': str(year), 'value': year} for year in data_year['db_an'].unique()],
                                value=data_year['db_an'].max(),  # Par défaut, dernière année disponible
                                placeholder="Sélectionnez une année",
                                className="me-2",
                            ),
                        )
                    ], align="center", justify="center"),
                ], style={
                    "backgroundColor": "blue",
                    "padding": "10px",
                    "borderRadius": "5px"
                }),
                width={"size": 6, "offset": 3}  # Centrer la colonne
            )
        ),
        # dbc.Row(
        #     dbc.Col(
        #         html.Div(
        #             children=[
        #                 html.Div(children="Année du décès", className="menu-title"),
        #                 # Dropdown pour sélectionner l'année
        #                 dcc.Dropdown(
        #                     id='year-dropdown',
        #                     options=[{'label': str(i), 'value': str(i)} for i in data_year['db_an']],
        #                     value=str(one_year['db_an'].iloc[0])  # Valeur par défaut
        #                 ),
        #             ],
        #             className="menu",
        #         ),
        #     ),
        # ),
        html.Br(),

        # Ligne avec les indicateurs décès
        dbc.Row([
            # Indicateur pour le nombre total
            dbc.Col(
                dbc.Button("Total décès", id="total-indicator-death", color="danger", className="me-1",
                           style={'width': '100%'}),
                width=4
            ),

            # Indicateur pour le nombre d'hommes
            dbc.Col(
                dbc.Button("Total décès hommes", id="homme-indicator-death", color="primary", className="me-1",
                           style={'width': '100%'}),
                width=4
            ),

            # Indicateur pour le nombre de femmes
            dbc.Col(
                dbc.Button("Total naissances femmes", id="femme-indicator-death", color="warning",
                           style={'width': '100%'}),
                width=4
            ),
        ], justify="center", className="my-3"),
        #
        # Ligne avec les indicateurs naissances
        dbc.Row([
            # Indicateur pour le nombre total
            dbc.Col(
                dbc.Button("Total naissances et morts dans l'année", id="total-indicator-birth", color="danger", className="me-1",
                           style={'width': '100%'}),
                width=4
            ),

            # Indicateur pour le nombre d'hommes
            dbc.Col(
                dbc.Button("Naissances et morts des hommes dans l'année", id="homme-indicator-birth", color="primary", className="me-1",
                           style={'width': '100%'}),
                width=4
            ),

            # Indicateur pour le nombre de femmes
            dbc.Col(
                dbc.Button("Naissances et morts des femmes dans l'année", id="femme-indicator-birth", color="warning", style={'width': '100%'}),
                width=4
            ),
        ], justify="center", className="my-3"),
        #
        html.Br(),
        # Boites à moustaches
        dbc.Row([
            dbc.Col(dcc.Graph(id="fig-moustache"), width=12)
        ], justify="center"),
        html.Br(),
        #
        dbc.Row(
            dbc.Col(
                dcc.Graph(id="birth-chart", config={"displayModeBar": False}),
                className="card",
            ),
        ),
        dbc.Row(
            dbc.Col(
                dcc.Graph(id="death-chart-month", config={"displayModeBar": False}),
                className="card",
            ),
        ),
        dbc.Row(
            dbc.Col(
                dcc.Graph(id="death-chart-year", config={"displayModeBar": False}),
                className="card",
            ),
        ),
        # boutons radios pour choix du sexe dans la carte choropleth
        dbc.Row([
            dbc.Col(
                dbc.RadioItems(
                    id='sex-radio',
                    options=[
                        {'label': 'Hommes', 'value': 'M'},
                        {'label': 'Femmes', 'value': 'F'},
                        {'label': 'Total', 'value': 'Total'}
                    ],
                    value='Total',  # Valeur par défaut
                    inline=True,
                    className="btn-group",
                    labelClassName="btn btn-outline-primary",
                    inputClassName="btn-check",
                ),
                width=12,
                className="d-flex justify-content-center my-3"
            )
        ]),
        # carte choropleth naissance
        dbc.Row(
            dbc.Col(
                dcc.Graph(id='choropleth-map-birth', config={"displayModeBar": False}, style={'height': '1000px'}),
                className="card",
                style={'height': '1000px'}  # Hauteur appliquée à la colonne
            ),
            style={'height': '1000px'}  # Hauteur appliquée à la rangée
        ),
        # carte choropleth décès
        dbc.Row(
            dbc.Col(
                dcc.Graph(id='choropleth-map-death', config={"displayModeBar": False}, style={'height': '1000px'}),
                className="card",
                style={'height': '1000px'}  # Hauteur appliquée à la colonne
            ),
            style={'height': '1000px'}  # Hauteur appliquée à la rangée
        ),
    ],
    fluid=True,
)


# Callback pour mettre à jour le graphique en fonction de l'année sélectionnée et le sexe pour la carte choropleth
@app.callback(
    Output('total-indicator-death', 'children'),
    Output('homme-indicator-death', 'children'),
    Output('femme-indicator-death', 'children'),
    Output('total-indicator-birth', 'children'),
    Output('homme-indicator-birth', 'children'),
    Output('femme-indicator-birth', 'children'),
    Output("fig-moustache", "figure"),
    Output('birth-chart', 'figure'),
    Output('death-chart-month', 'figure'),
    Output('death-chart-year', 'figure'),
    Output('choropleth-map-birth', 'figure'),
    Output('choropleth-map-death', 'figure'),
    [Input('year-dropdown', 'value'),
     Input('sex-radio', 'value')]
)

def update_graph(selected_year,selected_sexe):

    #print(f"Main : {selected_sexe}")

    # Connexion à la base de données SQLite
    conn = sqlite3.connect(db_path)

    # Extraire les données pour l'année sélectionnée
    df_birth = get_birth_by_year(selected_year, conn)
    df_death_month = get_death_by_month(selected_year, conn)
    df_death_year = get_death_by_year(selected_year, conn)

    # Dates pour les boites a moustaches
    df_boite_moustache = get_age_by_year_sexe(selected_year, conn)

    # Datas pour la carte choropleth des naissances et convertion en DataFrame Pandas
    df_choro_birth = pd.DataFrame(get_birth_by_year_dept(selected_year, selected_sexe, conn))

    # Datas pour la carte choropleth des décès et convertion en DataFrame Pandas
    df_choro_death = pd.DataFrame(get_death_by_year_dept(selected_year, selected_sexe, conn))

    #print(df_choro_birth.dtypes)
    #print(df_choro_birth)

    # Fermeture de la connexion
    conn.close()

    # 3 indicateurs des deces
    total_enreg_death, hommes_death, femmes_death = calculate_indicators(df_death_month)

    # 3 indicateurs de naissances
    total_enreg_birth, hommes_birth, femmes_birth= calculate_indicators(df_birth)

    #print(total_enreg_birth)

    # Créer les graphiques d'indicateurs deces
    fig_indic_death_total = go.Figure(go.Indicator(
        mode="number",
        value=total_enreg_death,
        number={'valueformat': '### ###'},  # Formatage avec espaces
        title={"text": "Total naissances et morts dans l'année"}
    ))

    fig_indic_death_men = go.Figure(go.Indicator(
        mode="number",
        value=hommes_death,
        number={'valueformat': '### ###'},  # Formatage avec espaces
        title={"text": "Total naissances et morts des hommes dans l'année"}
    ))

    # Ajouter un fond bleu avec `paper_bgcolor`
    fig_indic_death_men.update_layout(
        paper_bgcolor="lightblue",  # Définir la couleur de fond en bleu clair
        margin=dict(l=20, r=20, t=50, b=20)  # Ajuster les marges
    )

    fig_indic_death_women = go.Figure(go.Indicator(
        mode="number",
        value=femmes_death,
        number={'valueformat': '### ###'},  # Formatage avec espaces
        title={"text": "Total naissances et morts des femmes dans l'année"}
    ))

    # Créer les graphiques d'indicateurs naissances
    fig_indic_birth_total = go.Figure(go.Indicator(
        mode="number",
        value=total_enreg_birth,
        number={'valueformat': '### ###'},  # Formatage avec espaces
        title={"text": "Total naissances"}
    ))

    fig_indic_birth_men = go.Figure(go.Indicator(
        mode="number",
        value=hommes_birth,
        number={'valueformat': '### ###'},  # Formatage avec espaces
        title={"text": "Nombre d'Hommes"}
    ))

    # Ajouter un fond bleu avec `paper_bgcolor`
    fig_indic_birth_men.update_layout(
        paper_bgcolor="lightblue",  # Définir la couleur de fond en bleu clair
        margin=dict(l=20, r=20, t=50, b=20)  # Ajuster les marges
    )

    fig_indic_birth_women = go.Figure(go.Indicator(
        mode="number",
        value=femmes_birth,
        number={'valueformat': '### ###'},  # Formatage avec espaces
        title={"text": "Nombre de Femmes"}
    ))

    ######### Boite à moustaches
    fig_moustache = go.Figure()

    # Boîte pour toutes les données combinées
    fig_moustache.add_trace(
        go.Box(
            y=df_boite_moustache['age'],
            name="Total",
            boxmean='sd',
            marker_color='red'
        )
    )

    # Boîtes pour chaque sexe
    # Boîte pour les hommes_birth (s'il y en a dans les données)
    if 'M' in df_boite_moustache['sexe'].unique():
        fig_moustache.add_trace(
            go.Box(
                y=df_boite_moustache[df_boite_moustache['sexe'] == 'M']['age'],
                name="Homme",
                boxmean='sd',
                marker_color='blue'
            )
        )

    # Boîte pour les femmes (s'il y en a dans les données)
    if 'F' in df_boite_moustache['sexe'].unique():
        fig_moustache.add_trace(
            go.Box(
                y=df_boite_moustache[df_boite_moustache['sexe'] == 'F']['age'],
                name="Femme",
                boxmean='sd',
                marker_color='orange'
            )
        )

    # Ajout du titre dans le layout du graphique
    fig_moustache.update_layout(
        title="Statistiques autour de l'âge : min, max, moyenne, etc.",
        title_x=0.5,  # Centrer le titre
        title_font=dict(size=20)  # Taille de la police du titre
    )

    # Créer le graphique en barres des naissances
    fig_birth = go.Figure()

    # Tracer les données pour chaque sexe
    for sexe in df_birth['sexe'].unique():
        df_sexe = df_birth[df_birth['sexe'] == sexe]
        fig_birth.add_trace(go.Bar(
            x=df_sexe['db_mois'],
            y=df_sexe['Nbre'],
            name=f'Sexe : {sexe}',
            marker_color=color_map[sexe],  # Appliquer la couleur spécifiée
            # Infobulle personnalisée
            hovertemplate='<b>Mois: %{x}</b><br>Nombre de naissances: %{y}<br>Sexe: ' + sexe + '<extra></extra>'
        ))

        # barmode='group'

    fig_birth.update_layout(
        title=f"Nombre de naissances par mois et sexe pour l'année {selected_year}",
        xaxis_title="Mois",
        yaxis_title="Nombre de naissances",
        xaxis=dict(tickmode='array', tickvals=df_birth['db_mois'].unique(), ticktext=[
            'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']),
        template="plotly_white"
    )

    # Créer le graphique en barres des deces par mois
    fig_death_mois = go.Figure()

    # Tracer les données pour chaque sexe
    for sexe in df_death_month['sexe'].unique():
        df_sexe = df_death_month[df_death_month['sexe'] == sexe]
        fig_death_mois.add_trace(go.Bar(
            x=df_sexe['dd_mois'],
            y=df_sexe['Nbre'],
            name=f'Sexe : {sexe}',
            marker_color=color_map[sexe],  # Appliquer la couleur spécifiée
            # Infobulle personnalisée
            hovertemplate='<b>Mois: %{x}</b><br>Nombre de décès : %{y}<br>Sexe: ' + sexe + '<extra></extra>'
        ))

        # barmode='group'

    fig_death_mois.update_layout(
        title=f"Nombre de décès par mois et sexe pour l'année {selected_year}",
        xaxis_title="Mois",
        yaxis_title="Nombre de décès",
        xaxis=dict(tickmode='array', tickvals=df_death_month['dd_mois'].unique(), ticktext=[
            'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']),
        template="plotly_white"
    )

    # Créer le graphique en barres des deces par mois
    fig_death_year = go.Figure()

    # Tracer les données pour chaque sexe
    for sexe in df_death_year['sexe'].unique():
        df_sexe = df_death_year[df_death_year['sexe'] == sexe]
        fig_death_year.add_trace(go.Bar(
            x=df_sexe['db_an'],
            y=df_sexe['Nbre'],
            name=f'Sexe : {sexe}',
            marker_color=color_map[sexe],  # Appliquer la couleur spécifiée
            # Infobulle personnalisée
            hovertemplate='<b>Année: %{x}</b><br>Nombre de décès: %{y}<br>Sexe: ' + sexe + '<extra></extra>'
        ))

    fig_death_year.update_layout(
        title=f"Repartition des années de naissance selon l'année de décès : {selected_year}",
        xaxis_title="Annee",
        yaxis_title="Nombre de naissances",
        #xaxis=dict(tickmode='array', tickvals=df_death_year['dd_an'].unique(), ticktext=[
        #    'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']),
        template="plotly_white"
    )

    # Carte choropleth des naissances en France

    # Selecteur du sexe
    selected_color = color_map_choro.get(selected_sexe, 'Total')

    # carte des naissances
    # "#print(selected_color)
    fig_choro_birth = go.Figure(go.Choropleth(
        geojson=geojson_data,
        locations=df_choro_birth['departement'],  # Codes INSEE des départements
        featureidkey='properties.code',
        z=df_choro_birth['naissances'],  # Nombre de naissances
        #color=df_choro_birth['naissances'],  # Nombre de naissances
        locationmode='geojson-id',
        text=df_choro_birth['nom_departement'],  # Nom du département
        colorscale=selected_color,  # Palette de couleurs
        marker_line_color='black',  # Couleur des bordures
        colorbar_title="Naissances",
        )
    )


    # Mise à jour des contours et des limites géographiques
    fig_choro_birth.update_layout(
        title_text=f"Carte des naissances par département en France en {selected_year} avec un décès l'année même",
        geo=dict(
            showframe=False,  # Ne pas afficher les bordures
            showcoastlines=False,  # Ne pas afficher les côtes
            projection_type='mercator',  # Projection Mercator
            fitbounds="locations",
            # Définir manuellement les limites géographiques pour centrer sur la France
            #lataxis=dict(range=[41, 51]),  # Limites de latitude pour la France
            #lonaxis=dict(range=[-5, 9]),  # Limites de longitude pour la France
            visible=False,  # Désactiver l'affichage des axes
            resolution=50,  # Ajuste la précision du tracé des frontières (plus grand = plus détaillé)
        ),
        autosize=True,  # Permet de remplir l'espace disponible
        dragmode=False,  # Désactive le mode de glisser
        margin={"r":0,"t":50,"l":0,"b":0}  # Supprime les marges pour maximiser l'espace de la carte
    )

    # carte des deces
    # "#print(selected_color)
    fig_choro_death = go.Figure(go.Choropleth(
        geojson=geojson_data,
        locations=df_choro_death['departement'],  # Codes INSEE des départements
        featureidkey='properties.code',
        z=df_choro_death['deces'],  # Nombre de deces
        locationmode='geojson-id',
        text=df_choro_death['nom_departement'],  # Nom du département
        colorscale=selected_color,  # Palette de couleurs
        marker_line_color='black',  # Couleur des bordures
        colorbar_title="Décès",
    )
    )

    # Mise à jour des contours et des limites géographiques
    fig_choro_death.update_layout(
        title_text=f"Carte des décès par département en France en {selected_year}",
        geo=dict(
            showframe=False,  # Ne pas afficher les bordures
            showcoastlines=False,  # Ne pas afficher les côtes
            projection_type='mercator',  # Projection Mercator
            fitbounds="locations",
            # Définir manuellement les limites géographiques pour centrer sur la France
            # lataxis=dict(range=[41, 51]),  # Limites de latitude pour la France
            # lonaxis=dict(range=[-5, 9]),  # Limites de longitude pour la France
            visible=False,  # Désactiver l'affichage des axes
            resolution=50,  # Ajuste la précision du tracé des frontières (plus grand = plus détaillé)
        ),
        autosize=True,  # Permet de remplir l'espace disponible
        dragmode=False,  # Désactive le mode de glisser
        margin={"r": 0, "t": 50, "l": 0, "b": 0}  # Supprime les marges pour maximiser l'espace de la carte
    )

    #return fig_indic_birth_total, fig_indic_birth_men, fig_indic_birth_women, fig_birth, fig_death_mois, fig_death_year
    #return fig_indic_birth_total, fig_indic_birth_men, fig_indic_birth_women, fig_birth, fig_death_mois, fig_death_year, fig_choro_birth
    return f"Total décès: {total_enreg_death}", f"Décès hommes: {hommes_death}", f"Décès femmes: {femmes_death}",f"Total Naissances: {total_enreg_birth}", f"Naissances Garçons: {hommes_birth}", f"Naissances Filles: {femmes_birth}", fig_moustache, fig_birth, fig_death_mois, fig_death_year, fig_choro_birth, fig_choro_death

# Exécution de l'application
if __name__ == '__main__':
    app.run_server(debug=True, use_reloader=True)

