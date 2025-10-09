from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.mysql.hooks.mysql import MySqlHook
from sqlalchemy import text
from sqlalchemy.dialects.mysql import TINYINT, DOUBLE, VARCHAR, SMALLINT, INTEGER
import pandas as pd
import numpy as np
import json
import os
import re
import unicodedata
from rapidfuzz import fuzz
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.auth.transport.requests import Request

TOKEN_PATH = "/opt/airflow/dags/token.json"   
FOLDER_ID  = "1wlc8q98XUC4zrN-FdCqVULPnDur9o7ia"              
OUT_PATH   = "/opt/airflow/dags/data/spotify_grammy_full.csv"

@dag(
    dag_id="etl_pipeline",
    start_date=datetime(year=2025, month=10, day=1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "grammys", "etl"]
)
def etl_pipeline():

    @task()
    def extract_spotify_csv():
        file_path = "/opt/airflow/data/spotify_dataset.csv"
        df_spotify = pd.read_csv(file_path)
        print(df_spotify.head())
        return df_spotify.to_json(orient='records')

    @task()
    def extract_grammy_db():
        mysql_hook = MySqlHook(mysql_conn_id='mysql_local')
        sql = "SELECT * FROM grammy_awards;"
        df_grammy = mysql_hook.get_pandas_df(sql)
        print(df_grammy.head())
        return df_grammy.to_json(orient='records')

    @task()
    def transform_and_merge(spotify_json, grammy_json):
        df = pd.DataFrame(json.loads(spotify_json))
        df1 = pd.DataFrame(json.loads(grammy_json))

        # load spotify csv
        df = pd.read_csv("data/spotify_dataset.csv")

        # drop helper index column if present
        if "Unnamed: 0" in df.columns:
            df = df.drop(columns=["Unnamed: 0"])

        # drop unwanted columns (ignore if missing)
        cols_to_drop = ["key", "mode", "time_signature", "winner"]
        df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors="ignore")

        # drop rows with any nulls
        df = df.dropna().drop_duplicates(keep="first")
        print("Spotify shape after initial cleaning:", df.shape)

        # normalize synonyms in track_genre
        normalize_mapping = {"latino": "latin", "kids": "children"}
        df["track_genre"] = df["track_genre"].replace(normalize_mapping)

        # map sub-genres to main buckets
        genre_mapping = {
            # rock
            "alt-rock": "rock", "hard-rock": "rock", "punk-rock": "rock",
            "rock-n-roll": "rock", "rockabilly": "rock", "grunge": "rock",
            "psych-rock": "rock", "punk": "rock", "rock": "rock",
            # metal
            "metal": "metal", "black-metal": "metal", "death-metal": "metal",
            "heavy-metal": "metal", "metalcore": "metal", "grindcore": "metal",
            # pop
            "pop": "pop", "indie-pop": "pop", "power-pop": "pop",
            "synth-pop": "pop", "pop-film": "pop", "k-pop": "pop",
            "j-pop": "pop", "mandopop": "pop", "cantopop": "pop",
            # electronic
            "edm": "electronic", "electro": "electronic", "electronic": "electronic",
            "deep-house": "electronic", "detroit-techno": "electronic", "techno": "electronic",
            "house": "electronic", "progressive-house": "electronic", "chicago-house": "electronic",
            "trance": "electronic", "dubstep": "electronic", "drum-and-bass": "electronic",
            "idm": "electronic", "trip-hop": "electronic", "minimal-techno": "electronic",
            "club": "electronic", "dance": "electronic", "dancehall": "electronic",
            "disco": "electronic", "dub": "electronic", "garage": "electronic",
            "breakbeat": "electronic",
            # hip hop / r&b
            "hip-hop": "hip-hop", "r-n-b": "hip-hop",
            # jazz / blues
            "jazz": "jazz", "blues": "blues", "bluegrass": "blues",
            # latin
            "latin": "latin", "salsa": "latin", "samba": "latin", "pagode": "latin",
            "sertanejo": "latin", "brazil": "latin", "forro": "latin", "mpb": "latin",
            # other mains
            "country": "country", "folk": "folk", "gospel": "gospel",
            "opera": "classical", "classical": "classical", "piano": "classical",
            "acoustic": "acoustic", "singer-songwriter": "acoustic", "songwriter": "acoustic",
            # j-music
            "anime": "j-music", "j-rock": "j-music", "j-idol": "j-music", "j-dance": "j-music",
            # misc
            "alternative": "alternative", "ambient": "ambient", "world-music": "world",
            "afrobeat": "world", "indian": "world", "iranian": "world", "turkish": "world",
            "swedish": "world", "french": "world", "german": "world", "spanish": "world",
            "malay": "world", "emo": "emo", "hardcore": "hardcore", "hardstyle": "hardstyle",
            "industrial": "industrial", "goth": "goth", "groove": "groove",
            "funk": "funk", "soul": "soul", "comedy": "comedy", "children": "children",
            "disney": "children", "study": "study", "sleep": "study", "happy": "mood",
            "sad": "mood", "romance": "mood", "party": "mood", "show-tunes": "theatre",
            "new-age": "new-age", "chill": "chill", "guitar": "instrumental",
        }

        # create sub_genre and main_genre
        df["sub_genre"] = df["track_genre"]
        df["main_genre"] = df["track_genre"].map(genre_mapping).fillna(df["track_genre"])
        df = df.drop(columns=["track_genre"])

        # resolve duplicates by track_id
        def resolve_duplicates(group):
            main_row = group.loc[group["popularity"].idxmax()].copy()
            main_genre = main_row["main_genre"]
            subgenres = set(group["sub_genre"].dropna().unique())
            subgenres = {g for g in subgenres if g != main_genre}
            main_row["sub_genre"] = ", ".join(sorted(subgenres)) if subgenres else None
            return main_row

        df = df.groupby("track_id", group_keys=False, dropna=False).apply(resolve_duplicates).reset_index(drop=True)

        # duration in minutes
        df["duration_ms"] = df["duration_ms"] / 60000
        df = df.rename(columns={"duration_ms": "duration_min"})

        # clip positive loudness to 0
        df.loc[df["loudness"] > 0, "loudness"] = 0

        # normalize text columns
        for col in ["artists", "album_name", "track_name"]:
            df[col] = df[col].str.lower().str.strip()

        # consolidate by track_name + artists
        def consolidate_simple(group):
            main_row = group.loc[group["popularity"].idxmax()].copy()
            other_albums = group.loc[group["album_name"] != main_row["album_name"], "album_name"].unique().tolist()
            main_row["album_others"] = "; ".join(other_albums) if other_albums else None
            return main_row

        df = df.groupby(["track_name", "artists"], group_keys=False).apply(consolidate_simple).reset_index(drop=True)

        # load grammys csv
        df1 = pd.read_csv("data/the_grammy_awards.csv")

        # drop unused columns
        df1 = df1.drop(columns=["winner", "workers", "img", "published_at", "updated_at"], errors="ignore")

        # normalize text
        grammy_clean_cols = ["title", "category", "nominee", "artist"]
        for col in grammy_clean_cols:
            df1[col] = df1[col].astype(str).str.lower().str.strip()

        # fill empty nominee/artist
        for col in ["nominee", "artist"]:
            df1[col] = df1[col].replace("nan", "").replace("", "not specified").fillna("not specified")

        # helpers
        def normalize_artists(artist_str):
            if pd.isna(artist_str) or str(artist_str).strip() == "":
                return []
            s = unicodedata.normalize("NFKD", str(artist_str)).encode("ascii", "ignore").decode("utf-8").lower()
            s = re.sub(r"\s*(feat\.?|featuring|with|and|,|&|;)\s*", ";", s)
            return [a.strip() for a in s.split(";") if a.strip()]

        def is_various_artists(artist_str):
            if pd.isna(artist_str) or str(artist_str).strip() == "":
                return False
            s = unicodedata.normalize("NFKD", str(artist_str)).encode("ascii", "ignore").decode("utf-8").lower().strip()
            s = re.sub(r"\s+", " ", s)
            aliases = {"various artists", "varios artistas", "v.a.", "v.a", "varios", "various", "artistas varios", "compilation", "compilacion"}
            return s in aliases or bool(re.search(r"\b(various|varios)\s+artists?\b", s))

        def fuzzy_title_match(t1, t2, threshold=90):
            if not isinstance(t1, str) or not isinstance(t2, str):
                return False
            return fuzz.token_set_ratio(t1, t2) >= threshold

        def fuzzy_artist_match(list1, list2, threshold=90):
            if not list1 or not list2:
                return False
            for a1 in list1:
                for a2 in list2:
                    if fuzz.token_set_ratio(a1, a2) >= threshold:
                        return True
            return False

        # build artist lists and flags
        df["artist_list_spotify"] = df["artists"].apply(normalize_artists)
        df1["artist_list_grammy"] = df1["artist"].apply(normalize_artists)
        df1["is_various_artists"] = df1["artist"].apply(is_various_artists)

        # merge
        merged_full = pd.merge(
            df,
            df1,
            left_on="track_name",
            right_on="nominee",
            how="outer",
            suffixes=("_spotify", "_grammy")
        )

        # fuzzy match
        def row_is_match(row, title_thr=90, artist_thr=90):
            t_sp = row.get("track_name")
            t_gr = row.get("nominee")
            if not (isinstance(t_sp, str) and isinstance(t_gr, str)):
                return False
            title_ok = fuzzy_title_match(t_sp, t_gr, threshold=title_thr)
            if not title_ok:
                return False
            if bool(row.get("is_various_artists")):
                artists_sp = row.get("artist_list_spotify", []) or []
                return len(artists_sp) > 1
            artists_sp = row.get("artist_list_spotify", []) or []
            artists_gr = row.get("artist_list_grammy", []) or []
            return fuzzy_artist_match(artists_sp, artists_gr, threshold=artist_thr)

        merged_full["grammy_nominee"] = merged_full.apply(row_is_match, axis=1)

        # handle nans and types
        if "explicit" in merged_full.columns:
            merged_full["explicit"] = merged_full["explicit"].astype("boolean").fillna(False)
        else:
            merged_full["explicit"] = False

        obj_cols = [c for c in merged_full.select_dtypes(include=["object"]).columns if c != "explicit"]
        merged_full[obj_cols] = merged_full[obj_cols].fillna("not specified")

        num_cols = merged_full.select_dtypes(include=[np.number]).columns.tolist()
        merged_full[num_cols] = merged_full[num_cols].fillna(0)

        # rename and drop helper cols
        merged_full["artist_spotify"] = merged_full.get("artists", None)
        merged_full["artist_grammy"] = merged_full.get("artist", None)

        merged_full = merged_full.drop(
            columns=["artists", "artist", "artist_list_spotify", "artist_list_grammy", "is_various_artists"],
            errors="ignore"
        )

        # reorder columns
        cols_spotify = [
            "track_id", "track_name", "artist_spotify", "album_name", "album_others",
            "popularity", "duration_min", "explicit", "danceability", "energy", "loudness",
            "speechiness", "acousticness", "instrumentalness", "liveness", "valence", "tempo",
            "main_genre", "sub_genre"
        ]
        cols_grammy = ["year", "title", "category", "nominee", "artist_grammy", "grammy_nominee"]
        cols_spotify = [c for c in cols_spotify if c in merged_full.columns]
        cols_grammy = [c for c in cols_grammy if c in merged_full.columns]
        cols_rest = [c for c in merged_full.columns if c not in cols_spotify + cols_grammy]
        merged_full = merged_full[cols_spotify + cols_grammy + cols_rest]

        # safe print and return
        matched_rows = int(merged_full["grammy_nominee"].sum()) if "grammy_nominee" in merged_full.columns else 0
        print("Final merged shape:", merged_full.shape)
        print("Matched rows (grammy_nominee == True):", matched_rows)

        os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)

        for c in merged_full.select_dtypes(include=["boolean", "bool"]).columns:
            merged_full[c] = merged_full[c].astype(bool)

        merged_full.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")
        print(f"CSV saved in: {OUT_PATH}")
        return OUT_PATH  

    @task()
    def load_to_drive(file_path: str):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"No existe el archivo: {file_path}")

        creds = Credentials.from_authorized_user_file(TOKEN_PATH)
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())

        service = build('drive', 'v3', credentials=creds)

        metadata = {"name": os.path.basename(file_path), "mimeType": "text/csv"}
        if FOLDER_ID and FOLDER_ID != "TU_FOLDER_ID_AQUI":
            metadata["parents"] = [FOLDER_ID]

        media = MediaFileUpload(file_path, mimetype="text/csv", resumable=True)

        uploaded = service.files().create(
            body=metadata, media_body=media, fields="id, name, parents"
        ).execute()

        print(f"File uploaded to Drive: {uploaded.get('name')} (ID: {uploaded.get('id')})")
        return uploaded.get("id")


    @task()
    def load_star_schema(file_path: str,
                        schema_name: str = "full_dw",
                        recreate_schema: bool = True):

        # Read CSV 
        df = pd.read_csv(file_path)

        # Normalizes booleans if they come as text
        if "explicit" in df.columns:
            df["explicit"] = (
                df["explicit"].astype(str).str.strip().str.lower()
                .map({"true": True, "1": True, "false": False, "0": False})
                .fillna(False)
            )
        if "grammy_nominee" in df.columns:
            df["grammy_nominee"] = (
                df["grammy_nominee"].astype(str).str.strip().str.lower()
                .map({"true": True, "1": True, "false": False, "0": False})
                .fillna(False)
            )

        # MySQL connection
        hook = MySqlHook(mysql_conn_id="mysql_dw")
        engine = hook.get_sqlalchemy_engine()

        # DDL
        ddl = []
        if recreate_schema:
            ddl.append(f"DROP DATABASE IF EXISTS {schema_name}")
        ddl.append(f"CREATE DATABASE IF NOT EXISTS {schema_name}")

        ddl += [
            
            f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.dim_track (
                track_key INT AUTO_INCREMENT PRIMARY KEY,
                track_spotify_id VARCHAR(64) NOT NULL UNIQUE,
                track_name TEXT NOT NULL
            ) ENGINE=InnoDB
            DEFAULT CHARSET=utf8mb4
            COLLATE=utf8mb4_unicode_ci;
            """,

            f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.dim_artist (
                artist_key INT AUTO_INCREMENT PRIMARY KEY,
                artist_spotify TEXT NOT NULL,
                INDEX idx_artist_name (artist_spotify(191))
            ) ENGINE=InnoDB
            DEFAULT CHARSET=utf8mb4
            COLLATE=utf8mb4_unicode_ci;
            """,

            f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.dim_album (
                album_key INT AUTO_INCREMENT PRIMARY KEY,
                album_name   TEXT NOT NULL,
                album_others TEXT NOT NULL,
                INDEX idx_album (album_name(191), album_others(191))
            ) ENGINE=InnoDB
            DEFAULT CHARSET=utf8mb4
            COLLATE=utf8mb4_unicode_ci
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.dim_genre (
                genre_key INT AUTO_INCREMENT PRIMARY KEY,
                main_genre VARCHAR(100) NOT NULL,
                sub_genre  VARCHAR(100) NOT NULL,
                UNIQUE(main_genre, sub_genre)
            ) ENGINE=InnoDB;
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.dim_time (
                time_key INT AUTO_INCREMENT PRIMARY KEY,
                year SMALLINT NOT NULL UNIQUE
            ) ENGINE=InnoDB;
            """,

            f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.dim_grammy (
                grammy_key INT AUTO_INCREMENT PRIMARY KEY,
                title         VARCHAR(191) NOT NULL,
                category      VARCHAR(128) NOT NULL,
                nominee       VARCHAR(191) NOT NULL,
                artist_grammy TEXT NOT NULL,
                grammy_uk CHAR(64)
                AS (SHA2(CONCAT_WS('|', title, category, nominee, artist_grammy), 256)) STORED,
                UNIQUE KEY uq_dim_grammy (grammy_uk)
            ) ENGINE=InnoDB
            DEFAULT CHARSET=utf8mb4
            COLLATE=utf8mb4_unicode_ci;
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.fact_track_metrics (
                fact_id BIGINT AUTO_INCREMENT PRIMARY KEY,
                track_key  INT NOT NULL,
                artist_key INT NOT NULL,
                album_key  INT NOT NULL,
                genre_key  INT NOT NULL,
                time_key   INT NOT NULL,
                grammy_key INT NOT NULL,
                popularity       DOUBLE NOT NULL,
                duration_min     DOUBLE NOT NULL,
                explicit         TINYINT(1) NOT NULL,
                danceability     DOUBLE NOT NULL,
                energy           DOUBLE NOT NULL,
                loudness         DOUBLE NOT NULL,
                speechiness      DOUBLE NOT NULL,
                acousticness     DOUBLE NOT NULL,
                instrumentalness DOUBLE NOT NULL,
                liveness         DOUBLE NOT NULL,
                valence          DOUBLE NOT NULL,
                tempo            DOUBLE NOT NULL,
                grammy_nominee   TINYINT(1) NOT NULL,
                CONSTRAINT fk_track  FOREIGN KEY (track_key)  REFERENCES {schema_name}.dim_track(track_key),
                CONSTRAINT fk_artist FOREIGN KEY (artist_key) REFERENCES {schema_name}.dim_artist(artist_key),
                CONSTRAINT fk_album  FOREIGN KEY (album_key)  REFERENCES {schema_name}.dim_album(album_key),
                CONSTRAINT fk_genre  FOREIGN KEY (genre_key)  REFERENCES {schema_name}.dim_genre(genre_key),
                CONSTRAINT fk_time   FOREIGN KEY (time_key)   REFERENCES {schema_name}.dim_time(time_key),
                CONSTRAINT fk_grammy FOREIGN KEY (grammy_key) REFERENCES {schema_name}.dim_grammy(grammy_key)
            ) ENGINE=InnoDB;
            """,
        ]

        with engine.begin() as conn:
            for stmt in ddl:
                conn.execute(text(stmt))

        # NOT NULL
        for col in ["album_name","album_others","title","category","nominee",
                    "artist_grammy","artist_spotify","track_name","main_genre","sub_genre"]:
            if col in df.columns:
                df[col] = df[col].fillna("")
        if "year" in df.columns:
            df["year"] = df["year"].fillna(0).astype("int16")

        # Dimensions
        dim_track = df[["track_id","track_name"]].drop_duplicates().rename(columns={"track_id":"track_spotify_id"})
        dim_artist = df[["artist_spotify"]].drop_duplicates()
        dim_album  = df[["album_name","album_others"]].drop_duplicates()
        dim_genre  = df[["main_genre","sub_genre"]].drop_duplicates()
        dim_time   = df[["year"]].drop_duplicates().astype({"year":"int16"})
        dim_grammy = df[["title","category","nominee","artist_grammy"]].drop_duplicates()

        dim_track.to_sql("dim_track", engine, schema=schema_name, if_exists="append", index=False)
        dim_artist.to_sql("dim_artist", engine, schema=schema_name, if_exists="append", index=False)
        dim_album.to_sql("dim_album", engine, schema=schema_name, if_exists="append", index=False)
        dim_genre.to_sql("dim_genre", engine, schema=schema_name, if_exists="append", index=False)
        dim_time.to_sql("dim_time", engine, schema=schema_name, if_exists="append", index=False)
        dim_grammy.to_sql("dim_grammy", engine, schema=schema_name, if_exists="append", index=False)

        # Keys
        with engine.connect() as conn:
            t  = pd.read_sql(text(f"SELECT track_key, track_spotify_id FROM {schema_name}.dim_track"), conn)
            a  = pd.read_sql(text(f"SELECT artist_key, artist_spotify FROM {schema_name}.dim_artist"), conn)
            al = pd.read_sql(text(f"SELECT album_key, album_name, album_others FROM {schema_name}.dim_album"), conn)
            g  = pd.read_sql(text(f"SELECT genre_key, main_genre, sub_genre FROM {schema_name}.dim_genre"), conn)
            tm = pd.read_sql(text(f"SELECT time_key, year FROM {schema_name}.dim_time"), conn)
            gr = pd.read_sql(text(f"SELECT grammy_key, title, category, nominee, artist_grammy FROM {schema_name}.dim_grammy"), conn)

        # Fact
        fact = (df.copy()
                .merge(t,  left_on="track_id", right_on="track_spotify_id")
                .merge(a,  on="artist_spotify")
                .merge(al, on=["album_name","album_others"])
                .merge(g,  on=["main_genre","sub_genre"])
                .merge(tm, on="year")
                .merge(gr, on=["title","category","nominee","artist_grammy"]))

        if "explicit" in fact.columns:
            fact["explicit"] = fact["explicit"].astype(int)
        if "grammy_nominee" in fact.columns:
            fact["grammy_nominee"] = fact["grammy_nominee"].astype(int)

        fact_cols = [
            "track_key","artist_key","album_key","genre_key","time_key","grammy_key",
            "popularity","duration_min","explicit","danceability","energy","loudness",
            "speechiness","acousticness","instrumentalness","liveness","valence","tempo",
            "grammy_nominee"
        ]

        fact[fact_cols].to_sql("fact_track_metrics", engine, schema=schema_name,
                                if_exists="append", index=False, chunksize=10_000)

        return f"Cargado esquema {schema_name} con {len(fact)} filas en fact_track_metrics."

    
    # Orchestration
    spotify_data = extract_spotify_csv()
    grammy_data  = extract_grammy_db()
    csv_path     = transform_and_merge(spotify_data, grammy_data)
    load_to_drive(csv_path)
    load_star_schema(csv_path)

etl_pipeline()