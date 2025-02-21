import requests
from bs4 import BeautifulSoup

# URL yang akan di-scrape
url = 'https://www.soccerstats.com/results.asp?league=italy&pmtype=bydate'

# Mengirim permintaan GET ke URL
response = requests.get(url)

# Menggunakan BeautifulSoup untuk mem-parsing HTML
soup = BeautifulSoup(response.text, 'html.parser')

# Menemukan semua baris <tr> yang memiliki kelas "odd"
rows = soup.find_all('tr', {'class': 'odd'})

matches = []

# Loop melalui setiap baris yang ditemukan
for row in rows:
    cols = row.find_all('td')

    # Pastikan ada cukup kolom untuk mengambil data
    if len(cols) >= 9:
        # Ambil tanggal
        date = cols[0].get_text(strip=True)

        # Ambil tim kandang
        home_team = cols[1].get_text(strip=True)

        # Ambil skor
        score = cols[2].get_text(strip=True)

        # Ambil tim tandang
        away_team = cols[3].get_text(strip=True)

        # Cek jika baris tersebut adalah salah satu dari 'Totals', 'Averages', atau 'Percentages'
        if home_team in ['Totals', 'Averages', 'Percentages'] or not home_team.isalpha():
            continue  # Lewatkan baris ini jika merupakan 'Totals', 'Averages', atau 'Percentages'

        # Menyimpan data pertandingan ke dalam list
        matches.append({
            'date'     : date,
            'home_team': home_team,
            'score'    : score,
            'away_team': away_team
        })

# Cetak hasil data yang telah diambil
for match in matches:
    print(match)

# Menggabungkan dengan Spark untuk analisis data
import pyspark
import os
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

# Inisialisasi SparkSession
spark = SparkSession.builder \
    .appName("SoccerStats") \
    .getOrCreate()

# Mengubah data menjadi DataFrame Spark
matches_df = spark.createDataFrame(matches)

# Menampilkan 5 baris pertama
matches_df.show(5)

# Analisis data pertandingan
# Menghitung jumlah pertandingan per tim
home_team_count = matches_df.groupBy("home_team").count().withColumnRenamed("count", "home_matches")
away_team_count = matches_df.groupBy("away_team").count().withColumnRenamed("count", "away_matches")

# Menggabungkan jumlah pertandingan tim tuan rumah dan tim tamu
team_matches_count = home_team_count.join(away_team_count, home_team_count.home_team == away_team_count.away_team, "outer") \
    .select(
        F.coalesce(home_team_count.home_team, away_team_count.away_team).alias("team"),
        F.coalesce(home_team_count.home_matches, F.lit(0)).alias("home_matches"),
        F.coalesce(away_team_count.away_matches, F.lit(0)).alias("away_matches")
    )

# Menampilkan jumlah pertandingan per tim
team_matches_count.show()

# Menghitung total skor per tim
total_scores = matches_df.withColumn("home_score", F.split("score", "-").getItem(0).cast("int")) \
    .withColumn("away_score", F.split("score", "-").getItem(1).cast("int"))

total_scores_df = total_scores.groupBy("home_team").agg(
    F.sum("home_score").alias("total_home_score"),
    F.sum("away_score").alias("total_away_score")
)

# Menampilkan total skor per tim
total_scores_df.show()

# Menyimpan hasil ke CSV
def save_df_to_csv(spark_df, folder_path, filename):
    pandas_df = spark_df.toPandas()
    
    # Menentukan folder path di dalam folder yang dapat diakses
    os.makedirs(folder_path, exist_ok=True)
    
    # Menyimpan ke CSV menggunakan Pandas
    file_path = os.path.join(folder_path, f"{filename}.csv")
    pandas_df.to_csv(file_path, index=False)

    print(f"Data berhasil disimpan ke {file_path}")
    return file_path

# Menyimpan hasil ke CSV
save_df_to_csv(team_matches_count, "airflow/data", "team_matches_count")
save_df_to_csv(total_scores_df, "airflow/data", "total_scores")