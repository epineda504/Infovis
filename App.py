import pandas as pd
import pandasql as sql
import plotly.graph_objects as go
import plotly.io as pio

df_NFL_Playoffs = 'https://raw.githubusercontent.com/epineda504/Infovis/24ffb11d1914cfea0f18c7f9d503e7b86222beed/nfl_playoffs.csv'
df_NFL_Playoffs = pd.read_csv(df_NFL_Playoffs)

df_NFL_Teams = 'https://raw.githubusercontent.com/epineda504/Infovis/c2a7af009c97f62899e127380d4669d95fd4cf57/nfl_teams.csv'
df_NFL_Teams = pd.read_csv(df_NFL_Teams)

logos = {
    "Arizona Cardinals": "ari",
    "Phoenix Cardinals": "ari",
    "Atlanta Falcons": "atl",
    "Baltimore Colts": "ind",
    "Baltimore Ravens": "bal",
    "Boston Patriots": "ne",
    "Buffalo Bills": "buf",
    "Carolina Panthers": "car",
    "Chicago Bears": "chi",
    "Cincinnati Bengals": "cin",
    "Cleveland Browns": "cle",
    "Dallas Cowboys": "dal",
    "Denver Broncos": "den",
    "Detroit Lions": "det",
    "Green Bay Packers": "gb",
    "Houston Oilers": "ten",
    "Houston Texans": "hou",
    "Indianapolis Colts": "ind",
    "Jacksonville Jaguars": "jax",
    "Kansas City Chiefs": "kc",
    "Las Vegas Raiders": "lv",
    "Los Angeles Raiders": "lv",
    "Oakland Raiders": "lv",
    "Los Angeles Chargers": "lac",
    "San Diego Chargers": "lac",
    "Los Angeles Rams": "lar",
    "St. Louis Rams": "lar",
    "Miami Dolphins": "mia",
    "Minnesota Vikings": "min",
    "New England Patriots": "ne",
    "New Orleans Saints": "no",
    "New York Giants": "nyg",
    "New York Jets": "nyj",
    "Philadelphia Eagles": "phi",
    "Pittsburgh Steelers": "pit",
    "San Francisco 49ers": "sf",
    "Seattle Seahawks": "sea",
    "St. Louis Cardinals": "ari",
    "Tampa Bay Buccaneers": "tb",
    "Tennessee Oilers": "ten",
    "Tennessee Titans": "ten",
    "Washington Commanders": "wsh",
    "Washington Football Team": "wsh",
    "Washington Redskins": "wsh"
}

active_teams = {
    "Arizona Cardinals",
    "Atlanta Falcons",
    "Baltimore Ravens",
    "Buffalo Bills",
    "Carolina Panthers",
    "Chicago Bears",
    "Cincinnati Bengals",
    "Cleveland Browns",
    "Dallas Cowboys",
    "Denver Broncos",
    "Detroit Lions",
    "Green Bay Packers",
    "Houston Texans",
    "Indianapolis Colts",
    "Jacksonville Jaguars",
    "Kansas City Chiefs",
    "Las Vegas Raiders",
    "Los Angeles Chargers",
    "Los Angeles Rams",
    "Miami Dolphins",
    "Minnesota Vikings",
    "New England Patriots",
    "New Orleans Saints",
    "New York Giants",
    "New York Jets",
    "Philadelphia Eagles",
    "Pittsburgh Steelers",
    "San Francisco 49ers",
    "Seattle Seahawks",
    "Tampa Bay Buccaneers",
    "Tennessee Titans",
    "Washington Commanders"
}


def get_logo(team):
    code = logos.get(team)
    return f"https://a.espncdn.com/i/teamlogos/nfl/500/{code}.png" if code else None

def is_active(team):
    return 1 if team in active_teams else 0

col = df_NFL_Teams.columns[0]

df_NFL_Teams["logo_url"] = df_NFL_Teams[col].apply(get_logo)
df_NFL_Teams["flag_active"] = df_NFL_Teams[col].apply(is_active)

Pre_data = '''

SELECT 
strftime('%Y-%m-%d', 
    substr(schedule_date, -4) || '-' || 
    printf('%02d', substr(schedule_date, 1, instr(schedule_date, '/') - 1)) || '-' || 
    printf('%02d', substr(schedule_date, instr(schedule_date, '/') + 1, 
      instr(substr(schedule_date, instr(schedule_date, '/') + 1), '/') - 1))
      ) AS schedule_date

,a.schedule_week
,b.team_id as team_id_home
,a.team_home
,b.team_name_short as team_name_short_home
,b.team_division as team_division_home
,b.team_conference as team_conference_home
,b.logo_url as logo_url_home

,c.team_id as team_id_away
,a.team_away
,c.team_name_short as team_name_short_away
,c.team_division as team_division_away
,c.team_conference as team_conference_away
,c.logo_url as logo_url_away

,Cast(a.score_home as Integer) as score_home
,Cast(a.score_away as Integer) as score_away
,a.stadium
,a.stadium_neutral
,a.weather_temperature
,(a.weather_temperature - 32) / 1.8 as  weather_temperature_celsius

,a.weather_wind_mph
,a.weather_humidity

,(Case When a.team_home = 'Tampa Bay Buccaneers' and a.score_home > a.score_away Then 1
       When a.team_away = 'Tampa Bay Buccaneers' and a.score_away > a.score_home Then 1  
       Else 0 End) as Flag_Victory

,(Case When a.team_home = 'Tampa Bay Buccaneers' and a.score_home < a.score_away Then 1
       When a.team_away = 'Tampa Bay Buccaneers' and a.score_away < a.score_home Then 1  
       Else 0 End) as Flag_Defeat

,(Case When a.team_home = 'Tampa Bay Buccaneers'  Then a.score_home 
       When a.team_away = 'Tampa Bay Buccaneers'  Then a.score_away End) as Flag_Score

       
,(Case When a.team_home = 'Tampa Bay Buccaneers' and a.score_home > a.score_away Then 1
       When a.team_away = 'Tampa Bay Buccaneers' and a.score_away > a.score_home Then 1
       
       When a.team_home = 'Tampa Bay Buccaneers' and a.score_home < a.score_away Then (ABS(RANDOM()) % 100) / 100.0
       When a.team_away = 'Tampa Bay Buccaneers' and a.score_away < a.score_home Then (ABS(RANDOM()) % 100) / 100.0
       Else 0 End) as Personal_Satisfaction_Rate

FROM df_NFL_Playoffs as a
Left Join df_NFL_Teams as b on a.team_home = b.team_name
Left Join df_NFL_Teams as c on a.team_away = c.team_name
Where a.team_home = 'Tampa Bay Buccaneers' or a.team_away = 'Tampa Bay Buccaneers'

'''

Pre_data1 = sql.sqldf(Pre_data, locals())
Pre_data1.to_csv('NFL_Buccaneers_Data.csv', index=False)




#----------------------------------------------------------------------------------------------#

# import pandas as pd
# import plotly.graph_objects as go
# import plotly.io as pio

# # Suponiendo que ya tienes Pre_data1 cargado
# # Pre_data1 = pd.read_csv("buccaneers_playoffs.csv")

# # Convertir fechas
# Pre_data1["schedule_date"] = pd.to_datetime(Pre_data1["schedule_date"], errors="coerce")

# # Usar Flag_Score como métrica principal
# Pre_data1["score_flag"] = Pre_data1["Flag_Score"]

# # Crear columna de texto para hover
# Pre_data1["hover_text"] = Pre_data1["Flag_Victory"].apply(lambda x: "Victoria" if x == 1 else "Derrota")

# # Separar victorias y derrotas
# victorias = Pre_data1[Pre_data1["Flag_Victory"] == 1]
# derrotas = Pre_data1[Pre_data1["Flag_Victory"] == 0]

# # Gráfico
# fig = go.Figure()

# # Línea gris general
# fig.add_trace(go.Scatter(
#     x=Pre_data1["schedule_date"],
#     y=Pre_data1["score_flag"],
#     mode="lines",
#     line=dict(color="gray", width=2),
#     name="Score Line",
#     hoverinfo="skip"
# ))

# # Puntos verdes (victorias)
# fig.add_trace(go.Scatter(
#     x=victorias["schedule_date"],
#     y=victorias["score_flag"],
#     mode="markers",
#     name="Victoria",
#     marker=dict(size=8, color="green"),
#     text=victorias["hover_text"],
#     hovertemplate="Resultado: %{text}<br>Score: %{y}<br>Fecha: %{x}<extra></extra>"
# ))

# # Puntos rojos (derrotas)
# fig.add_trace(go.Scatter(
#     x=derrotas["schedule_date"],
#     y=derrotas["score_flag"],
#     mode="markers",
#     name="Derrota",
#     marker=dict(size=8, color="red"),
#     text=derrotas["hover_text"],
#     hovertemplate="Resultado: %{text}<br>Score: %{y}<br>Fecha: %{x}<extra></extra>"
# ))

# # Anotaciones importantes (solo texto, con saltos de línea para que no sean tan anchas)
# annotations = {
#     "2003-01-26": "Super Bowl XXXVII<br>(vs Raiders)",
#     "2020-03-20": "Llega Tom Brady<br>como agente libre",
#     "2021-02-07": "Super Bowl LV<br>(vs Chiefs)<br>Tom Brady MVP",
#     "2025-01-05": "Mike Evans<br>1,000 yardas<br>11ª temporada consecutiva"
# }

# for date, text in annotations.items():
#     target_date = pd.to_datetime(date)
#     row = Pre_data1.loc[Pre_data1["schedule_date"] == target_date]

#     max_y = Pre_data1["score_flag"].max()

#     if not row.empty:
#         y_val = row["score_flag"].values[0]
#     else:
#        y_val = max_y + 4   # valor fijo si no hay partido

#     annotation_text = f"{text}<br>Fecha: {target_date.strftime('%Y-%m-%d')}"


#     fig.add_annotation(
#         x=target_date,
#         y=y_val,
#         text=annotation_text,  # texto con fecha
#         showarrow=True,
#         arrowhead=3,
#         arrowcolor="gold",
#         ax=0,
#         ay=-140,
#         bgcolor="#1D6E8C",
#         font=dict(color="white", size=10, family="Arial"),
#         bordercolor="#1D6E8C",
#         borderpad=2,
#         align="center"  # centra el texto para que no se vea tan ancho
#     )

# # Configuración responsiva y título más grande
# pio.templates.default = "seaborn"
# fig.update_layout(
#     title="Tampa Bay Buccaneers - Momentos Importantes",
#     title_font=dict(size=32, color="Black", family="Arial"),
#     title_x=0.5,
#     xaxis_title="Fechas",
#     yaxis_title="Score",
#     xaxis=dict(
#         title_font=dict(size=18),
#         tickformat="%Y-%m",
#         dtick="M40"   # ticks cada 40 meses
#     ),
#     yaxis=dict(title_font=dict(size=18)),
#     hovermode="x unified",
#     autosize=True,
#     template=pio.templates.default
    
# )

# # Hoverlabel estético
# fig.update_traces(
#     hoverlabel=dict(
#         bgcolor="rgba(30,30,30,0.9)",   # fondo oscuro semitransparente
#         bordercolor="lightgray",        # borde claro
#         font=dict(color="white", size=14, family="Arial")
#     )
# )


# # Exportar a HTML
# fig.write_html("Plotly.html", include_plotlyjs="cdn", full_html=True)