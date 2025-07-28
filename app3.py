#
#
#

import sqlite3
import pandas as pd
import os
import plotly.graph_objects as go

from dash import Dash, dcc, html, Input, Output

# Construire le chemin absolu
db_path = os.path.abspath('../DATAS/DB/deces_sequential.db')
# print(db_path)

# Fonction pour extraire les données par année
def get_birth_by_year(year, laconn):

    # Connexion à la base de données SQLite
    conn = sqlite3.connect(db_path)

    # print("ICI")
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
color_map = {'M': 'blue', 'F': 'pink', 'Total': 'red'}

external_stylesheets = [
    {
        "href": (
            "https://fonts.googleapis.com/css2?"
            "family=Lato:wght@400;700&display=swap"
        ),
        "rel": "stylesheet",
    },
]

app = Dash(__name__, external_stylesheets=external_stylesheets)

app.title = "Analyse naissances et deces en France !"

# Layout de l'application avec un dropdown pour l'année et un graphique
app.layout = html.Div(
    children=[
        html.Div(
            children=[
                html.P(children="🥑", className="header-emoji"),
                html.H1(
                    children="Stats sur la population française", className="header-title"
                ),
                html.P(
                    children=(
                        " Statistiques des naissances et morts en France "
                        "Données issues des actes de deces depuis 1970"
                    ),
                    className="header-description",
                ),
            ],
            className="header",
        ),
        html.Div(
            children=[
                html.Div(
                    children=[
                        html.Div(children="Année", className="menu-title"),
                        # Dropdown pour sélectionner l'année
                        dcc.Dropdown(
                            id='year-dropdown',
                            options=[{'label': str(i), 'value': str(i)} for i in data_year['db_an']],
                            value=str(one_year['db_an'].iloc[0])  # Valeur par défaut
                        ),
                    ]
                ),
                # html.Div(
                #     children=[
                #         html.Div(children="Type", className="menu-title"),
                #         dcc.Dropdown(
                #             id="type-filter",
                #             options=[
                #                 {
                #                     "label": avocado_type.title(),
                #                     "value": avocado_type,
                #                 }
                #                 for avocado_type in avocado_types
                #             ],
                #             value="organic",
                #             clearable=False,
                #             searchable=False,
                #             className="dropdown",
                #         ),
                #     ],
                # ),
                # html.Div(
                #     children=[
                #         html.Div(
                #             children="Date Range", className="menu-title"
                #         ),
                #         dcc.DatePickerRange(
                #             id="date-range",
                #             min_date_allowed=data["Date"].min().date(),
                #             max_date_allowed=data["Date"].max().date(),
                #             start_date=data["Date"].min().date(),
                #             end_date=data["Date"].max().date(),
                #         ),
                #     ]
                # ),
            ],
            className="menu",
        ),
        html.Div(
            children=[
                # html.Div([
                #         dcc.Graph(id='total-indicator-birth', style={'width': '30%'}),
                #         dcc.Graph(id='homme-indicator-birth', style={'width': '30%'}),
                #         dcc.Graph(id='femme-indicator-birth', style={'width': '30%'})
                #     ], className="card",
                # ),
                html.Div([
                         dcc.Graph(id='total-indicator-birth', style={'heigth' : 80, 'width': 500}),
                         dcc.Graph(id='homme-indicator-birth', style={'heigth' : 80, 'width': 500}),
                         dcc.Graph(id='femme-indicator-birth', style={'heigth' : 80, 'width': 500})
                     ],  style={'display': 'flex', 'justify-content': 'space-around', 'flex-shrink': 90, 'margin-bottom': '24px'},
                ),
                html.Div(
                    children=dcc.Graph(
                        id="birth-chart",
                        config={"displayModeBar": False},
                    ),
                    className="card",
                ),
                html.Div(
                    children=dcc.Graph(
                        id="death-chart-month",
                        config={"displayModeBar": False},
                    ),
                    className="card",
                ),
                html.Div(
                    children=dcc.Graph(
                        id="death-chart-year",
                        config={"displayModeBar": False},
                    ),
                    className="card",
                ),
                # html.Div(
                #     children=dcc.Graph(
                #         id="volume-chart",
                #         config={"displayModeBar": False},
                #     ),
                #     className="card",
                # ),
            ],
            className="wrapper",
        ),
    ]
)


# Callback pour mettre à jour le graphique en fonction de l'année sélectionnée
@app.callback(
    Output('total-indicator-birth', 'figure'),
    Output('homme-indicator-birth', 'figure'),
    Output('femme-indicator-birth', 'figure'),
    Output('birth-chart', 'figure'),
    Output('death-chart-month', 'figure'),
    Output('death-chart-year', 'figure'),
    [Input('year-dropdown', 'value')]
)

def update_graph(selected_year):

    # Connexion à la base de données SQLite
    conn = sqlite3.connect(db_path)

    # Extraire les données pour l'année sélectionnée
    df_birth = get_birth_by_year(selected_year, conn)
    df_death_month = get_death_by_month(selected_year, conn)
    df_death_year = get_death_by_year(selected_year, conn)

    # print(df_birth)
    # print(df)

    # Fermeture de la connexion
    conn.close()

    # 3 indicateurs de naissances
    total_enregistrements, hommes, femmes = calculate_indicators(df_birth)

    #print(total_enregistrements)

    # Créer les graphiques d'indicateurs
    fig_indic_birth_total = go.Figure(go.Indicator(
        mode="number",
        value=total_enregistrements,
        number={'valueformat': '### ###'},  # Formatage avec espaces
        title={"text": "Total naissances"}
    ))

    fig_indic_birth_men = go.Figure(go.Indicator(
        mode="number",
        value=hommes,
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
        value=femmes,
        number={'valueformat': '### ###'},  # Formatage avec espaces
        title={"text": "Nombre de Femmes"}
    ))

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
            hovertemplate='<b>Mois: %{x}</b><br>Nombre de deces: %{y}<br>Sexe: ' + sexe + '<extra></extra>'
        ))

        # barmode='group'

    fig_death_mois.update_layout(
        title=f"Nombre de deces par mois et sexe pour l'année {selected_year}",
        xaxis_title="Mois",
        yaxis_title="Nombre de deces",
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
            hovertemplate='<b>Mois: %{x}</b><br>Nombre de deces: %{y}<br>Sexe: ' + sexe + '<extra></extra>'
        ))

    fig_death_year.update_layout(
        title=f"Repartition de l'année de naissance selon le deces pour l'année {selected_year}",
        xaxis_title="Annee",
        yaxis_title="Nombre de naissances",
        #xaxis=dict(tickmode='array', tickvals=df_death_year['dd_an'].unique(), ticktext=[
        #    'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']),
        template="plotly_white"
    )

    #return fig_indic_birth_total, fig_indic_birth_men, fig_indic_birth_women, fig_birth, fig_death_mois, fig_death_year
    return fig_indic_birth_total, fig_indic_birth_men, fig_indic_birth_women, fig_birth, fig_death_mois, fig_death_year

# Exécution de l'application
if __name__ == '__main__':
    app.run_server(debug=True, use_reloader=True)

