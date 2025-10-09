import mysql.connector
import csv

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="grammy_awards"
)

cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS grammy_awards (
    year INT,
    title TEXT,
    published_at VARCHAR(50),
    updated_at VARCHAR(50),
    category TEXT,
    nominee TEXT,
    artist TEXT,
    workers TEXT,
    img TEXT,
    winner BOOLEAN
)
""")

csv_path = r"data\the_grammy_awards.csv"

with open(csv_path, mode='r', encoding='utf-8-sig') as file:
    csv_reader = csv.DictReader(file)
    for row in csv_reader:
        cursor.execute("""
            INSERT INTO grammy_awards 
            (year, title, published_at, updated_at, category, nominee, artist, workers, img, winner)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            int(row['year']),
            row['title'],
            row['published_at'], 
            row['updated_at'],    
            row['category'],
            row['nominee'] if row['nominee'] else None,
            row['artist'] if row['artist'] else None,
            row['workers'] if row['workers'] else None,
            row['img'] if row['img'] else None,
            row['winner'].lower() == 'true'
        ))

conn.commit()
cursor.close()
conn.close()

print("Raw CSV successfully loaded into MySQL.")
