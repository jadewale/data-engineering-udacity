import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """Read a song file and
        insert data to songs and artists tables
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0]
    cur.execute(artist_table_insert, artist_data)
    
    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0]
    cur.execute(song_table_insert, song_data)
    
    

def process_log_file(cur, filepath):
    """Read a log file and
        insert data into time, users and songplays tables
    """
    # open log file
    df = pd.read_json(filepath, lines=True)
    # filter by NextSong action
    df =  df.loc[df['page'] == 'NextSong']
    
    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"])
    
    time_frame = []
    for index, row in t.items():
        hour = row.hour
        wk_year = row.weekofyear
        month = row.month
        year = row.year
        weekday = row.dayofweek
        single_time_frame = [row ,hour, wk_year, month, year, weekday]
        time_frame.append(single_time_frame)

        
    # insert time data records
    time_data = time_frame
    column_labels = ['time stamp','hour','week of year','month','year','weekday']
    time_df = pd.DataFrame(time_data,columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df.filter(["userId","firstName","lastName","gender","level"])
    
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts), row.userId, row.level, artistid, songid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()