# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

songplay_table_create = (""" 
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id SERIAL PRIMARY KEY,
        start_time TIMESTAMP,
        user_id VARCHAR(25) REFERENCES users(user_id),
        level VARCHAR(25),
        song_id VARCHAR(50) REFERENCES songs(song_id),
        artist_id VARCHAR(50) REFERENCES artists(artist_id),
        session_id INTEGER,
        location TEXT,
        user_agent TEXT,
        UNIQUE (start_time, user_id, session_id)
    )
""")

# user_id, first_name, last_name, gender, level
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id VARCHAR(25) primary key,
        first_name VARCHAR(25),
        last_name VARCHAR(25),
        gender VARCHAR(8),
        level VARCHAR(25)
    )
""")

# song_id, title, artist_id, year, duration
song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id VARCHAR(100) primary key,
        title VARCHAR(255),
        artist_id VARCHAR(50) REFERENCES artists(artist_id),
        year int,
        duration NUMERIC
    )
""")


# artist_id, name, location, latitude, longitude

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id varchar(100) primary key,
        name TEXT,
        location TEXT,
        latitude VARCHAR(100),
        longitude VARCHAR(100)
    )
""")

# start_time, hour, day, week, month, year, weekday
time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time TIMESTAMP,
        hour VARCHAR(10),
        week VARCHAR(10),
        month VARCHAR(12),
        year int,
        weekday VARCHAR(12)
    )
""")

# INSERT RECORDS

# cursor.execute("INSERT INTO a_table (c1, c2, c3) VALUES(%s, %s, %s)", (v1, v2, v3))

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, artist_id, song_id, session_id, location, user_agent) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s,%s,%s,%s,%s ) ON CONFLICT (user_id) DO UPDATE set level = EXCLUDED.level
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES(%s, %s, %s, %s, %s) ON CONFLICT (song_id) DO NOTHING
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES(%s, %s, %s, %s, %s) ON CONFLICT (artist_id) DO NOTHING
""")

# hour	day	week of year	month	year	weekday

time_table_insert = ("""
    INSERT INTO time (start_time, hour, week, month, year, weekday) VALUES(%s, %s, %s, %s, %s, %s) 
""")

# FIND SONGS

song_select = ("""
    SELECT s.song_id, s.artist_id FROM songs as s INNER JOIN artists as a ON s.artist_id = a.artist_id WHERE s.title = %s AND a.name = %s AND s.duration = %s
""")


# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create, song_table_create, songplay_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]