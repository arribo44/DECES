

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

# Créer une date pour chaque jour de l'année 2023
dates = pd.date_range(start='2020-01-01', end='2023-12-31')

# Générer des décès aléatoires entre 1 et 50 pour chaque date
deaths = np.random.randint(0, 51, size=len(dates))

# Créer le DataFrame
df = pd.DataFrame({'date': dates, 'deaths': deaths})

# Créer un DataFrame
#df = pd.DataFrame(data)

# Assure-toi que tu accèdes à un DataFrame
death_counts = df[['date', 'deaths']]  # Ceci crée un DataFrame

# Vérifier le type
#print(type(df))  # Devrait être <class 'pandas.core.frame.DataFrame'>

# Afficher les colonnes
#print(df.columns)  # Devrait afficher ['date', 'deaths']

#print(df.head())

# S'assurer que la colonne 'date' est au bon format
df['date'] = pd.to_datetime(df['date'])

# Initialiser l'application Dash
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Layout de l'application
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col(html.H3("Statistiques de décès par jour pour une année"), width="auto", style={'text-align': 'center'}),
        dbc.Col(dcc.Dropdown(
            id="year-dropdown",
            options=[{"label": str(year), "value": year} for year in range(2020, 2024)],
            value=2023,
            clearable=False,
            style={'width': '50%', 'margin': '0 auto'}
        ), width=4)
    ], justify="center"),

    dbc.Row([
        dbc.Col(dcc.Graph(id="calplot-deaths"), width=12)
    ])
], fluid=True)


# Callback pour mettre à jour le graphique en fonction de l'année sélectionnée
@app.callback(
    Output("calplot-deaths", "figure"),
    Input("year-dropdown", "value")
)
def update_calplot(selected_year):
    # Simuler des données de décès pour l'année sélectionnée (exemple statique)
    #dates = pd.date_range(start=f"{selected_year}-01-01", end=f"{selected_year}-12-31", freq="D")
    #death_counts = pd.Series([abs(int((date.day + date.month) * 1.5)) for date in dates], index=dates)

    filtered_df = df[df['date'].dt.year == selected_year].copy()



    # Créer le calendrier avec plotly_calplot
    fig = calplot.calplot(
        filtered_df,
        x='date',  # Colonne avec les dates
        y='deaths',  # Colonne avec les valeurs de décès
        colorscale="Blues",
        title=f"Décès quotidiens pour l'année {selected_year}",
        month_lines_width=1
    )

    # Ajustements de style pour le titre et l'apparence
    fig.update_layout(
        title_x=0.5,
        title_font=dict(size=20)
    )

    return fig


# Exécuter le serveur
if __name__ == "__main__":
    app.run_server(debug=True)
