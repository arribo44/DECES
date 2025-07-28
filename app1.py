import sqlite3
import pandas as pd
import os
import plotly.graph_objects as go

from dash import Dash, dcc, html

# Construire le chemin absolu
db_path = os.path.abspath('../DATAS/DB/deces_sequential.db')
#print(db_path)

# Connexion à la base de données SQLite
conn = sqlite3.connect(db_path)

# Requête SQL pour extraire toutes les données
sql_all = """
SELECT db_mois, COUNT(*) as Nbre
FROM personnes
where db_an = 1970
GROUP BY db_mois
ORDER BY db_mois
"""

# Charger les données dans un DataFrame Pandas
df_all = pd.read_sql_query(sql_all, conn)

# Requête SQL pour extraire toutes les données par sexe
sql_sexe= """
SELECT db_mois, sexe, COUNT(*) as Nbre
FROM personnes
where db_an = 1970
GROUP BY db_mois, sexe
ORDER BY db_mois
"""

# Charger les données dans un DataFrame Pandas
df_sexe = pd.read_sql_query(sql_sexe, conn)

# Calculer le total de naissances par mois
totals = df_sexe.groupby('db_mois')['Nbre'].sum().reset_index()

# Ajouter une colonne 'sexe' pour marquer les lignes de total
totals['sexe'] = 'Total'

# Fusionner les totaux avec le DataFrame original
df_sexe = pd.concat([df_sexe, totals[['db_mois', 'sexe', 'Nbre']]], ignore_index=True)

# Trier le DataFrame par 'db_mois' pour maintenir l'ordre
df_sexe = df_sexe.sort_values(by=['db_mois', 'sexe'], ascending=[True, False]).reset_index(drop=True)


# Fermeture de la connexion
conn.close()

# Définir les couleurs pour chaque sexe
#color_map = {1: 'blue', 2: 'pink'}
color_map = {'M': 'blue', 'F': 'pink', 'Total': 'red'}


# Remplacement des valeurs numériques par des labels
df_sexe['sexe'] = df_sexe['sexe'].replace({1: 'M', 2: 'F'})
print(df_sexe)
#print(type(df_sexe))
#
# Création d'un graphique en barres pour le nombre de naissances par mois
fig_all = go.Figure()

# Création d'un graphique en barres pour le nombre de naissances par mois
fig_sexe = go.Figure()

# Ajout des barres au graphique en utilisant les colonnes db_mois (x) et Nbre (y)
fig_all.add_trace(go.Bar(x=df_all['db_mois'], y=df_all['Nbre'], name='Nombre de naissances'))

# Ajouter des traces pour chaque sexe
for sexe in df_sexe['sexe'].unique():
    print(sexe)
    df = df_sexe[df_sexe['sexe'] == sexe]
    print(df)
    fig_sexe.add_trace(go.Bar(
        x=df['db_mois'],
        y=df['Nbre'],
        name=f'Sexe : {sexe}',
        marker_color=color_map[sexe],  # Appliquer la couleur spécifiée
        hovertemplate='<b>Mois: %{x}</b><br>Nombre de naissances: %{y}<br>Sexe: '+sexe+'<extra></extra>'  # Infobulle personnalisée
    ))

# Mise en page du graphique
fig_all.update_layout(
    title="Nombre de naissances par mois",
    xaxis_title="Mois",
    yaxis_title="Nombre de naissances",
    xaxis=dict(tickmode='array', tickvals=df_all['db_mois'], ticktext=[
        'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']),
    template="plotly_white"
)

fig_sexe.update_layout(
    title="Nombre de naissances par mois et sexe",
    xaxis_title="Mois",
    yaxis_title="Nombre de naissances",
    xaxis=dict(tickmode='array', tickvals=df_sexe['db_mois'].unique(), ticktext=[
        'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']),
    template="plotly_white"
)


# Initialisation de l'application Dash
app = Dash(__name__)

# Définition de la mise en page de l'application
app.layout = html.Div(children=[
    html.H1(children='Nbre de naissance par mois selon année'),

    dcc.Graph(
        figure=fig_all
    ),

    dcc.Graph(
        figure=fig_sexe
    ),

])

# Exécution de l'application
if __name__ == '__main__':
    app.run_server(debug=True,use_reloader=True)