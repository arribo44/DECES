import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import pandas as pd

# Exemple de données
data = {"nom": ["Alice", "Bob", "Charlie", "David"], "age": [25, 32, 40, 29]}
df = pd.DataFrame(data)
age_moyen = df['age'].mean()

# Initialiser l'application Dash avec Bootstrap
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Layout de l'application avec des onglets
app.layout = dbc.Container([
    dcc.Tabs(id="tabs", value='tab-1', children=[
        dcc.Tab(label='Exemple 1', value='tab-1'),
        dcc.Tab(label='Exemple 2', value='tab-2'),
        dcc.Tab(label='Exemple 3', value='tab-3'),
        dcc.Tab(label='Exemple 4', value='tab-4'),
        dcc.Tab(label='Exemple 6', value='tab-6'),
        dcc.Tab(label='Exemple 7', value='tab-7'),
        dcc.Tab(label='Exemple 8', value='tab-8'),
        dcc.Tab(label='Exemple 9', value='tab-9'),
        dcc.Tab(label='Exemple 10', value='tab-10'),
    ]),
    html.Div(id='tabs-content')
])

# Callback pour afficher le contenu de chaque onglet
@app.callback(Output('tabs-content', 'children'), Input('tabs', 'value'))
def render_content(tab):
    if tab == 'tab-1':
        return dbc.Card(
            dbc.CardBody([
                html.H4("Âge Moyen - Style 1", className="card-title"),
                html.P(f"{age_moyen:.1f} ans", className="card-text", style={'fontSize': '24px'})
            ])
        )
    elif tab == 'tab-2':
        return dbc.Alert(f"L'âge moyen est de {age_moyen:.1f} ans.", color="info")
    elif tab == 'tab-3':
        return html.Div([
            html.H2("Âge Moyen - Style 3"),
            html.P(f"{age_moyen:.1f} ans", style={'color': 'blue', 'fontSize': '30px'})
        ])
    elif tab == 'tab-4':
        return html.Div([
            dbc.Badge(f"{age_moyen:.1f} ans", color="primary", className="me-1"),
            html.Span("Âge Moyen")
        ])
    elif tab == 'tab-6':
        return dbc.Card([
            dbc.CardHeader("Âge Moyen"),
            dbc.CardBody([
                html.H5(f"{age_moyen:.1f} ans", className="card-title"),
                html.P("Calculé à partir des données.")
            ])
        ])
    elif tab == 'tab-7':
        return html.Div([
            html.H4("Âge Moyen", style={'textDecoration': 'underline'}),
            html.P(f"{age_moyen:.1f} ans", style={'fontWeight': 'bold'})
        ])
    elif tab == 'tab-8':
        return dbc.Toast(
            [html.P(f"Âge moyen : {age_moyen:.1f} ans", className="mb-0")],
            header="Statistique",
            icon="info",
            style={"width": "100%"}
        )
    elif tab == 'tab-9':
        return dbc.Card([
            dbc.CardImg(src="https://via.placeholder.com/150", top=True),
            dbc.CardBody([
                html.H4("Âge Moyen", className="card-title"),
                html.P(f"{age_moyen:.1f} ans", className="card-text")
            ])
        ])
    elif tab == 'tab-10':
        return html.Div([
            dbc.Button(f"{age_moyen:.1f} ans", color="primary"),
            html.Span(" Âge Moyen affiché sur un bouton")
        ])

# Exécuter l'application
if __name__ == '__main__':
    app.run_server(debug=True)
