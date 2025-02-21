from airflow.decorators import dag, task
import bs4
import requests
import pandas as pd
import pytz
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sqlalchemy as sa
from sqlalchemy import inspect

# DATABASE_URL secara global
DATABASE_URL = "postgresql://user:password@host.docker.internal:5433/postgres_db"

# DAG tanpa menggunakan parameter
@dag(
    schedule_interval='0 9 * * *',  # Menjadwalkan setiap hari jam 9 pagi
    start_date=datetime(2025, 1, 1),  # Tanggal mulai DAG
    catchup=False  # Tidak melakukan catchup
)
def test_airflow_dag():

    # Task untuk memulai dan mengakhiri DAG
    start_task = EmptyOperator(task_id="start_task")
    end_task = EmptyOperator(task_id="end_task")

    # Task untuk mengekstrak data dari web
    @task
    def extract_web(league: str):
        url = f"https://www.soccerstats.com/results.asp?league={league}&pmtype=bydate"  # URL pertandingan dari beberapa liga eropa
        response = requests.get(url)  # Mendapatkan data dari URL
        soup = bs4.BeautifulSoup(response.content, "html.parser")  # Parsing HTML dengan BeautifulSoup

        rows = soup.find_all('tr', {'class': 'odd'})  # Mencari semua baris dengan class 'odd'
        matches = []

        for row in rows:
            cols = row.find_all('td')  # Mengambil semua kolom dari baris

            if len(cols) >= 4:
                date = cols[0].get_text(strip=True)
                home_team = cols[1].get_text(strip=True)
                score = cols[2].get_text(strip=True)
                away_team = cols[3].get_text(strip=True)

                # Melewati baris yang tidak relevan
                if date in ['Averages', 'Percentages'] or home_team in ['Totals']:
                    continue

                matches.append({
                    'date'     : date,
                    'home_team': home_team,
                    'score'    : score,
                    'away_team': away_team,
                    'league'   : league
                })

        return matches

    # Task untuk memproses data yang telah diekstrak
    @task
    def process_data(matches):
        processed_data = []
        today = datetime.today()  # Mendapatkan tanggal hari ini
        yesterday = today - timedelta(days=1)  # Menghitung tanggal H-1

        for match in matches:
            day, date_str = match['date'].split(' ', 1)  # Memisahkan hari dan tanggal
            day_full = {
                'Sa': 'Saturday',
                'Su': 'Sunday',
                'Mo': 'Monday',
                'Tu': 'Tuesday',
                'We': 'Wednesday',
                'Th': 'Thursday',
                'Fr': 'Friday'
            }.get(day, day)

            # Menggunakan hanya tanggal tanpa hari
            date_str = date_str.strip()  # Menghapus spasi ekstra

            # Menentukan tahun berdasarkan bulan dan tanggal
            month = date_str.split()[1]
            year = 2025 if month in ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul'] else 2024

            # Membentuk string tanggal lengkap
            date_str = f"{date_str.split()[0]} {month} {year}"

            # Mengonversi tanggal pertandingan ke format datetime
            try:
                match_date = datetime.strptime(date_str, '%d %b %Y')
            except ValueError as e:
                print(f"Error: {e}")
                continue

            # Cek apakah tanggal pertandingan lebih besar atau sama dengan tanggal hari ini
            if match_date >= yesterday:
                continue

            # Memproses skor
            score_parts = match['score'].split(' - ')
            home_score = int(score_parts[0]) if len(score_parts) > 1 else None
            away_score = int(score_parts[1]) if len(score_parts) > 1 else None

            processed_data.append({
                'day'       : day_full,
                'date'      : date_str,
                'home_team' : match['home_team'],
                'home_score': home_score,
                'away_team' : match['away_team'],
                'away_score': away_score,
                'league'    : match['league']
            })
            print(processed_data)

        # Membuat DataFrame dari data yang telah diproses
        df = pd.DataFrame(processed_data)
        df['id'] = range(1, len(df) + 1)

        # Menambahkan waktu pembuatan data dengan zona waktu Jakarta
        jakarta_tz = pytz.timezone('Asia/Jakarta')
        jakarta_now = datetime.now(jakarta_tz).strftime('%Y-%m-%d %H:%M:%S')
        df['created_at'] = jakarta_now

        cols = ['id', 'day', 'date', 'home_team', 'home_score', 'away_team', 'away_score', 'league', 'created_at']
        df = df[cols]

        # Menambahkan hasil pertandingan untuk analitik klasemen
        df['result_home'] = df.apply(lambda row: 3 if row['home_score'] > row['away_score'] else (1 if row['home_score'] == row['away_score'] else 0), axis=1)
        df['result_away'] = df.apply(lambda row: 3 if row['away_score'] > row['home_score'] else (1 if row['away_score'] == row['home_score'] else 0), axis=1)

        return df

    # Task untuk memuat data ke database PostgreSQL
    @task
    def load_database(df, league: str):
        try:
            # Menyusun nama tabel berdasarkan liga
            table_name = f"matches_{league}"  # Nama tabel berdasarkan liga

            # Membuat koneksi ke PostgreSQL
            engine = sa.create_engine(DATABASE_URL)
            
            # Cek apakah tabel matches sudah ada, jika ada maka di-drop dulu
            if sa.inspect(engine).has_table(table_name):
                with engine.connect() as connection:
                    connection.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
                print(f"Tabel '{table_name}' telah di-drop sebelum memasukkan data baru.")
            
            # Memuat data ke tabel sesuai nama tabel yang dibuat berdasarkan liga
            df.to_sql(table_name, engine, if_exists='replace', index=False)

            print(f"Data pertandingan {league} berhasil disimpan ke tabel '{table_name}' di PostgreSQL.")
        except Exception as e:
            print(f"Terjadi kesalahan saat menyimpan data ke PostgreSQL: {e}")
            raise

    # Task untuk menghitung klasemen dari data pertandingan
    @task
    def calculate_standings(df, league: str):
        standings = []
        
        created_at = df['created_at'].iloc[0] if not df.empty else datetime.now()

        for _, row in df.iterrows():
            home_team = row['home_team']
            away_team = row['away_team']
            home_score = row['home_score']
            away_score = row['away_score']

            # Mengupdate data tim di klasemen untuk home team
            home_team_exists = next((team for team in standings if team['club'] == home_team), None)
            if home_team_exists:
                home_team_exists['match'] += 1
                if home_score > away_score:
                    home_team_exists['win'] += 1
                    home_team_exists['points'] += 3
                elif home_score == away_score:
                    home_team_exists['draw'] += 1
                    home_team_exists['points'] += 1
                else:
                    home_team_exists['loss'] += 1
                home_team_exists['goal_for'] += home_score
                home_team_exists['goal_against'] += away_score
            else:
                standings.append({
                    'club'        : home_team,
                    'match'       : 1,
                    'win'         : 1 if home_score > away_score else 0,
                    'draw'        : 1 if home_score == away_score else 0,
                    'loss'        : 1 if home_score < away_score else 0,
                    'goal_for'    : home_score,
                    'goal_against': away_score,
                    'goal_diff'   : home_score - away_score,
                    'points'      : 3 if home_score > away_score else (1 if home_score == away_score else 0)
                })

            # Mengupdate data tim di klasemen untuk away team
            away_team_exists = next((team for team in standings if team['club'] == away_team), None)
            if away_team_exists:
                away_team_exists['match'] += 1
                if away_score > home_score:
                    away_team_exists['win'] += 1
                    away_team_exists['points'] += 3
                elif away_score == home_score:
                    away_team_exists['draw'] += 1
                    away_team_exists['points'] += 1
                else:
                    away_team_exists['loss'] += 1
                away_team_exists['goal_for'] += away_score
                away_team_exists['goal_against'] += home_score
            else:
                standings.append({
                    'club'        : away_team,
                    'match'       : 1,
                    'win'         : 1 if away_score > home_score else 0,
                    'draw'        : 1 if away_score == home_score else 0,
                    'loss'        : 1 if away_score < home_score else 0,
                    'goal_for'    : away_score,
                    'goal_against': home_score,
                    'goal_diff'   : away_score - home_score,
                    'points'      : 3 if away_score > home_score else (1 if away_score == home_score else 0)
                })

        standings_sorted = sorted(standings, key=lambda x: (
            x['points'], x['goal_for'], x['goal_against'], x['win'], x['draw'], x['loss']), reverse=True)

        standings_df = pd.DataFrame(standings_sorted)
        standings_df['id'] = range(1, len(standings_df) + 1)
        standings_df['goal_diff'] = standings_df['goal_for'] - standings_df['goal_against']

        standings_df['created_at'] = created_at

        standings_df = standings_df[['id', 'club', 'points', 'match', 'win', 'draw', 'loss', 'goal_for', 'goal_against', 'goal_diff', 'created_at']]

        # Menyusun nama tabel berdasarkan liga
        table_name = f"standings_{league}"

        # Cek apakah tabel standings sudah ada, jika ada maka di-drop dulu
        engine = sa.create_engine(DATABASE_URL)
        if sa.inspect(engine).has_table(table_name):
            with engine.connect() as connection:
                connection.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
            print(f"Tabel '{table_name}' telah di-drop sebelum memasukkan data baru.")
        
        standings_df.to_sql(table_name, engine, if_exists='replace', index=False)

        print(f"Klasemen {league} berhasil disimpan ke tabel '{table_name}' di PostgreSQL.")
        return standings_df

    # Task untuk memuat klasemen ke dalam database PostgreSQL
    @task
    def load_standings_to_db(standings_df, league: str):
        try:
            # Menyusun nama tabel berdasarkan liga
            table_name = f"standings_{league}"  # Nama tabel berdasarkan liga

            # Membuat koneksi ke PostgreSQL
            engine = sa.create_engine(DATABASE_URL)
            
            # Cek apakah tabel standings sudah ada, jika ada maka di-drop dulu
            if sa.inspect(engine).has_table(table_name):
                with engine.connect() as connection:
                    connection.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
                print(f"Tabel '{table_name}' telah di-drop sebelum memasukkan data baru.")
            
            # Memuat data ke tabel sesuai nama tabel yang dibuat berdasarkan liga
            standings_df.to_sql(table_name, engine, if_exists='replace', index=False)

            print(f"Data klasemen {league} berhasil disimpan ke tabel '{table_name}' di PostgreSQL.")
        except Exception as e:
            print(f"Terjadi kesalahan saat menyimpan data ke PostgreSQL: {e}")
            raise

    # Mendefinisikan URL liga
    leagues = ['england', 'france', 'italy', 'germany', 'netherlands', 'spain']

    # Menghubungkan task untuk setiap liga
    for league in leagues:
        matches = extract_web(league)
        processed_data = process_data(matches)
        load_db = load_database(processed_data, league)
        standings = calculate_standings(processed_data, league)
        load_standings = load_standings_to_db(standings, league)

        # Menyusun urutan task
        start_task >> matches >> processed_data >> load_db >> standings >> load_standings >> end_task

# Menjalankan DAG
test_airflow_dag()
