

import pandas as pd
import os
import plotly.graph_objects as go
import numpy as np

from dash import Dash, dcc, html, Input, Output
import dash_bootstrap_components as dbc

import plotly_calplot as calplot

# Données de décès : exemple de données pour une année spécifique
# (chaque jour aura un nombre aléatoire de décès pour illustration)
# Exemple de données
# data = {
#     'date': pd.date_range(start='2023-01-01', periods=5, freq='D'),  # Dates
#     'deaths': [5, 2, 0, 3, 8]  # Nombre de décès fictifs
# }

# Définir la couleur
color_map = {
    'H': 'blues',
    'F': 'purp',
    'Tous': 'gray'
}

# Créer une date pour chaque jour de l'année 2023
date_range = pd.date_range(start='2020-01-01', end='2023-12-31')

# Créer des DataFrames séparés pour homme et femme
data_men = {
    'date': date_range,
    'deaths': np.random.randint(1, 51, size=len(date_range)),  # Valeurs aléatoires entre 1 et 50
    'sexe': 'H'
}

data_women = {
    'date': date_range,
    'deaths': np.random.randint(1, 51, size=len(date_range)),  # Valeurs aléatoires entre 1 et 50
    'sexe': 'F'
}

# Créer des DataFrames
df_men = pd.DataFrame(data_men)
df_women = pd.DataFrame(data_women)

# Concaténer les DataFrames
df = pd.concat([df_men, df_women], ignore_index=True)

# S'assurer que la colonne 'date' est au bon format
df['date'] = pd.to_datetime(df['date'])

# Initialiser l'application Dash
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Layout de l'application
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.Label("Sélectionnez l'année :"),
            dcc.Dropdown(
                id='year-dropdown',
                options=[{'label': str(year), 'value': year} for year in range(2020, 2024)],
                value=2023,
                clearable=False,
            )
        ], width=4),
        dbc.Col([
            html.Label("Sélectionnez le sexe :"),
            dcc.RadioItems(
                id='sex-radio',
                options=[
                    {'label': 'Homme', 'value': 'H'},
                    {'label': 'Femme', 'value': 'F'},
                    {'label': 'Tous', 'value': 'Tous'}
                ],
                value='Tous',
                inline=True
            )
        ], width=4)
    ]),
    html.Br(),
    dbc.Row([
        dbc.Col(
            dcc.Graph(id='calplot-deaths'),
            className="card",
        )
    ])
], fluid=True)


# Callback pour mettre à jour le graphique en fonction de l'année sélectionnée
@app.callback(
    Output("calplot-deaths", "figure"),
    [Input('year-dropdown', 'value'),
     Input('sex-radio', 'value')]
)
def update_calplot(selected_year, selected_sex):
    # Simuler des données de décès pour l'année sélectionnée (exemple statique)
    #dates = pd.date_range(start=f"{selected_year}-01-01", end=f"{selected_year}-12-31", freq="D")
    #death_counts = pd.Series([abs(int((date.day + date.month) * 1.5)) for date in dates], index=dates)

    filtered_df = df[df['date'].dt.year == selected_year].copy()

    # Filtrer selon le sexe
    if selected_sex != 'Tous':
        filtered_df = filtered_df[filtered_df['sexe'] == selected_sex]

    # Définir la couleur
    selected_color = color_map[selected_sex]

    # Créer le calendrier avec plotly_calplot
    fig_calendar = calplot.calplot(
        filtered_df,
        x='date',  # Colonne avec les dates
        y='deaths',  # Colonne avec les valeurs de décès
        colorscale=selected_color,
        showscale=True,
        name="Décès",
        gap=8,
        title=f"Décès quotidiens pour l'année {selected_year}",
        month_lines_width=1
    )

    # Ajustements de style pour le titre et l'apparence
    fig_calendar.update_layout(
        title_x=0.5,
        title_y=1,
        title_font=dict(size=20)
    )

    return fig


# Exécuter le serveur
if __name__ == "__main__":
    app.run_server(debug=True)
