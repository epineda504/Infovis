import pandas as pd
import pandasql as sql

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

FROM df_NFL_Playoffs as a
Left Join df_NFL_Teams as b on a.team_home = b.team_name
Left Join df_NFL_Teams as c on a.team_away = c.team_name
Where a.team_home = 'Tampa Bay Buccaneers' or a.team_away = 'Tampa Bay Buccaneers'

'''

Pre_data1 = sql.sqldf(Pre_data, locals())
Pre_data1.to_csv('NFL_Buccaneers_Data.csv', index=False)

# print(Pre_data1.head())

## ------------------------------------------------------------------------------------------------------- ##

# =========================
# 1. CONFIG
# =========================
team_focus = "Tampa Bay Buccaneers"

# =========================
# 2. LEER CSV
# =========================
df = pd.read_csv("NFL_Buccaneers_Data.csv")

# =========================
# 3. FILTRAR PARTIDOS DEL EQUIPO
# =========================
df = df[
    (df['team_home'] == team_focus) |
    (df['team_away'] == team_focus)
]

# =========================
# 4. MAPEAR LOGOS
# =========================
team_logo_map = {}

for _, row in df.iterrows():
    if pd.notna(row['logo_url_home']):
        team_logo_map[row['team_home']] = row['logo_url_home']
    if pd.notna(row['logo_url_away']):
        team_logo_map[row['team_away']] = row['logo_url_away']

def get_logo(team):
    return team_logo_map.get(team, "https://via.placeholder.com/32")

# =========================
# 5. ORDENAR POR FECHA
# =========================
df['parsed_date'] = pd.to_datetime(df['schedule_date'], errors='coerce')
df = df.sort_values(by='parsed_date', ascending=False)

# =========================
# 6. GENERAR CARDS
# =========================
html_cards = ""

for _, row in df.iterrows():

    home = row['team_home']
    away = row['team_away']
    home_score = row['score_home']
    away_score = row['score_away']

    # Fecha formateada
    try:
        date = row['parsed_date'].strftime('%b %d, %Y')
    except:
        date = row['schedule_date']

    # Tipo de partido
    week_type = row.get('schedule_week', '')

    # Colores ganador
    if home_score > away_score:
        home_color = "white"
        away_color = "gray"
    else:
        home_color = "gray"
        away_color = "white"

    html_cards += f"""
    <div class="match">

        <div class="teams">
            <div class="team">
                <img src="{get_logo(home)}">
                <span style="color:{home_color}">{home}</span>
            </div>

            <div class="score">
                {home_score} - {away_score}
            </div>

            <div class="team right">
                <span style="color:{away_color}">{away}</span>
                <img src="{get_logo(away)}">
            </div>
        </div>

        <div class="details">
            {date} | {week_type}
        </div>

    </div>
    """

# =========================
# 7. HTML FINAL
# =========================
html_page = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<style>

body {{
    background-color: #0f1115;
    font-family: Arial;
    color: white;
}}

.container {{
    width: 800px;
    margin: 40px auto;
}}

h2 {{
    text-align: center;
}}

.header {{
    display: grid;
    grid-template-columns: 1fr auto 1fr;
    align-items: center;
    margin-bottom: 12px;
    padding: 0 10px;
    font-size: 16px;
    font-weight: bold;
    color: white;
    text-transform: uppercase;
    border-bottom: 1px solid #2a2e36;
    padding-bottom: 8px;
}}

.header div:nth-child(2) {{
    text-align: center;
}}

.header div:last-child {{
    text-align: right;
}}

.match {{
    background: #1a1d23;
    padding: 15px;
    margin: 12px 0;
    border-radius: 12px;
}}

.match:hover {{
    background: #2a2e36;
    transition: 0.2s;
}}

.teams {{
    display: grid;
    grid-template-columns: 1fr auto 1fr;
    align-items: center;
}}

.team {{
    display: flex;
    align-items: center;
    gap: 10px;
}}

.team.right {{
    justify-content: flex-end;
}}

.team img {{
    width: 32px;
    height: 32px;
}}

.score {{
    font-size: 26px;
    font-weight: bold;
    text-align: center;
    letter-spacing: 1px;
}}

.details {{
    margin-top: 8px;
    font-size: 12px;
    color: #aaa;
    text-align: center;
}}

</style>
</head>

<body>

<div class="container">
<h2>Game Log</h2>

<div class="header">
    <div>Home</div>
    <div>Score</div>
    <div>Away</div>
</div>

{html_cards}

</div>

</body>
</html>
"""

# =========================
# 8. GUARDAR HTML
# =========================
with open("game_log.html", "w", encoding="utf-8") as f:
    f.write(html_page)

print("✅ HTML generado correctamente con header")