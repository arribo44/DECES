import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd

# Initialiser l'application Dash
app = dash.Dash(__name__)

# Données pour les graphiques
df = px.data.iris()  # Exemple avec le dataset iris

# Layout de l'application avec des onglets
app.layout = html.Div([
    dcc.Tabs(id="tabs-example", value='tab-1', children=[
        dcc.Tab(label='Graphique 1', value='tab-1'),
        dcc.Tab(label='Graphique 2', value='tab-2'),
        dcc.Tab(label='Graphique 3', value='tab-3'),
    ]),
    html.Div(id='tabs-content')
])

# Callback pour afficher le contenu des onglets
@app.callback(
    Output('tabs-content', 'children'),
    [Input('tabs-example', 'value')]
)
def render_content(tab):
    if tab == 'tab-1':
        fig = px.scatter(df, x='sepal_width', y='sepal_length', color='species')
        return dcc.Graph(figure=fig)
    elif tab == 'tab-2':
        fig = px.histogram(df, x='sepal_length', color='species')
        return dcc.Graph(figure=fig)
    elif tab == 'tab-3':
        fig = px.box(df, x='species', y='petal_width')
        return dcc.Graph(figure=fig)

# Exécuter l'application
if __name__ == '__main__':
    app.run_server(debug=True)

