import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import pandas as pd
import plotly.graph_objects as go

# Dataset d'exemple
data = {
    'sexe': ['Homme', 'Femme', 'Homme', 'Femme', 'Homme', 'Femme', 'Homme', 'Femme', 'Homme', 'Femme', 'Homme', 'Femme', 'Homme', 'Femme'],
    'age': [25, 30, 45, 32, 27, 35, 29, 41, 33, 28,3,2,78,99]
}
df = pd.DataFrame(data)

# Calcul des statistiques
stats_df = df.groupby('sexe')['age'].agg(['min', 'max', 'mean', 'median'])
stats_df['Q1'] = df.groupby('sexe')['age'].quantile(0.25)
stats_df['Q3'] = df.groupby('sexe')['age'].quantile(0.75)

# Calcul des statistiques pour l'ensemble des données
total_stats = {
    'Min': df['age'].min(),
    'Max': df['age'].max(),
    'Moyenne': df['age'].mean(),
    'Médiane': df['age'].median(),
    'Q1': df['age'].quantile(0.25),
    'Q3': df['age'].quantile(0.75)
}

# Initialisation de l'application Dash avec un thème Bootstrap
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Création de la figure de boîte à moustache
fig = go.Figure()

# Boîtes pour chaque sexe
for sexe in df['sexe'].unique():
    fig.add_trace(
        go.Box(
            y=df[df['sexe'] == sexe]['age'],
            name=sexe,
            boxmean='sd',  # Affiche la moyenne
            marker_color='blue' if sexe == 'Homme' else 'pink'
        )
    )

# Boîte pour toutes les données combinées
fig.add_trace(
    go.Box(
        y=df['age'],
        name="Total",
        boxmean='sd',
        marker_color='grey'
    )
)

# Disposition de l'application Dash
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col(html.H3("Analyse statistique des âges par sexe et en totalité"), width=12)
    ], justify="center", className="my-3"),

    dbc.Row([
        dbc.Col(dcc.Graph(figure=fig), width=12)
    ], justify="center"),

    dbc.Row([
        dbc.Col([
            html.H5("Statistiques par sexe"),
            html.Div([
                html.P(f"{stat_name.capitalize()}: {value:.2f}")
                for stat_name, value in stats_df.loc['Homme'].items()
            ], style={"border": "1px solid #ddd", "padding": "10px", "border-radius": "5px", "margin-bottom": "10px"}),
            html.Div([
                html.P(f"{stat_name.capitalize()}: {value:.2f}")
                for stat_name, value in stats_df.loc['Femme'].items()
            ], style={"border": "1px solid #ddd", "padding": "10px", "border-radius": "5px", "margin-bottom": "10px"}),
            html.H5("Statistiques pour tous les âges"),
            html.Div([
                html.P(f"{stat_name}: {value:.2f}")
                for stat_name, value in total_stats.items()
            ], style={"border": "1px solid #ddd", "padding": "10px", "border-radius": "5px"})
        ])
    ])
])

# Lancer le serveur
if __name__ == '__main__':
    app.run_server(debug=True)
