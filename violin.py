import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
import dash_bootstrap_components as dbc

# Données d'exemple
data = {
    "age": [25, 40, 60, 35, 80, 55, 75, 85, 45, 30, 20, 90, 70, 65, 50] * 10,
    "sexe": ["H", "F"] * 75,
    "année": [2023] * 75 + [2022] * 75
}
df = pd.DataFrame(data)

# Création de l'application Dash avec un thème Bootstrap
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = dbc.Container([
    dbc.Row([
        dbc.Col(html.H1("Distribution des âges des décès", className="text-center text-primary mb-4"), width=12)
    ]),
    
    dbc.Row([
        dbc.Col([
            html.Label("Sélectionner l'année", className="fw-bold"),
            dcc.Dropdown(
                id='year-dropdown',
                options=[{'label': year, 'value': year} for year in df['année'].unique()],
                value=2023,  # Valeur par défaut
                className="mb-3"
            ),
        ], width=6),
        
        dbc.Col([
            html.Label("Sélectionner le sexe", className="fw-bold"),
            dbc.RadioItems(
                id='gender-radio',
                options=[
                    {'label': 'Tous', 'value': 'Tous'},
                    {'label': 'Homme', 'value': 'H'},
                    {'label': 'Femme', 'value': 'F'}
                ],
                value='Tous',
                inline=True,
                className="mb-3"
            ),
        ], width=6)
    ]),
    
    dbc.Row([
        dbc.Col(dcc.Graph(id="violin-plot"), width=12)
    ])
], fluid=True)

# Callback pour mettre à jour le graphique en fonction des sélections
@app.callback(
    Output("violin-plot", "figure"),
    [Input("year-dropdown", "value"), Input("gender-radio", "value")]
)
def update_violin_plot(selected_year, selected_gender):
    # Filtrer les données
    filtered_df = df[df['année'] == selected_year]
    
    # Filtrer par sexe si sélectionné
    if selected_gender != 'Tous':
        filtered_df = filtered_df[filtered_df['sexe'] == selected_gender]
    
    # Créer le graphique en violon
    fig = px.violin(
        filtered_df, 
        y="age", 
        color="sexe",
        box=True,
        points="all",
        title=f"Distribution des âges des décès pour l'année {selected_year} ({selected_gender})"
    )
    
    fig.update_layout(
        xaxis_title="Sexe",
        yaxis_title="Âge",
        violingap=0.5,
        template="plotly_white"
    )
    return fig

if __name__ == '__main__':
    app.run_server(debug=True)

