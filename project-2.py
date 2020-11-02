Part I. ETL Pipeline for Pre-Processing the Files
PLEASE RUN THE FOLLOWING CODE FOR PRE-PROCESSING THE FILES
Import Python packages
# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
Creating list of filepaths to process original event csv data files
# checking your current working directory
print(os.getcwd())
​
# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'
​
# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    #print(file_path_list)
/home/workspace
Processing the files to create the data file csv that will be used for Apache Casssandra tables
# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
# for every filepath in the file path list 
for f in file_path_list:
​
# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            
# uncomment the code below if you would like to get total number of rows 
#print(len(full_data_rows_list))
# uncomment the code below if you would like to check to see what the list of event data rows will look like
#print(full_data_rows_list)
​
# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)
​
with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))
​
# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))
6821
Part II. Complete the Apache Cassandra coding portion of your project.
Now you are ready to work with the CSV file titled event_datafile_new.csv, located within the Workspace directory. The event_datafile_new.csv contains the following columns:
artist
firstName of user
gender of user
item number in session
last name of user
length of the song
level (paid or free song)
location of the user
sessionId
song title
userId
The image below is a screenshot of what the denormalized data should appear like in the event_datafile_new.csv after the code above is run:

Image
Begin writing your Apache Cassandra code in the cells below
Creating a Cluster
from cassandra.cluster import Cluster
cluster = Cluster()
​
# To establish connection and begin executing queries, need a session
session = cluster.connect()
Create Keyspace
try:
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS udacity
        WITH REPLICATION = 
        { 'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
except Exception as e:
    print(e)
    
def executeQuery(session, query):
    try:
        session.execute(query)
    except Exception as e:
        print(e)
def db_rows_to_pandas(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)
def retrieve_pandas(columns, rows):
    return rows._current_rows[columns]
Set Keyspace
try:
    session.set_keyspace('udacity')
    session.row_factory = db_rows_to_pandas
except Exception as e:
    print(e)
    
Now we need to create tables to run the following queries. Remember, with Apache Cassandra you model the database tables on the queries you want to run.
Create queries to ask the following three questions of the data
1. Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4
2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
music_session_query = 'DROP TABLE IF EXISTS music_session'
​
# Clear Database for music session and create tables
executeQuery(session, music_session_query)
​
# Primary Key is made up of two fields. session_id is the partition key, while item_in_session is the clustering column. 
# Items will be grouped and stored by partition keys while rows are ordered by item_in_session which is why you always have to use a balanced partition key
music_session_query = 'CREATE TABLE IF NOT EXISTS music_session'
music_session_query = music_session_query + ' (session_id int, item_in_session int, artist text, song text, length float, PRIMARY KEY(session_id, item_in_session))'
​
# execute music session query
executeQuery(session, music_session_query)   
​
​
user_status = 'DROP TABLE IF EXISTS user_status';
# Clear Database for user session and create tables
executeQuery(session, user_status)
​
## Primary Key is made up of three fields. user_id is the partition key, while session_id, item_in_session are clustering columns which is how data will be ordered. 
user_status = 'CREATE TABLE IF NOT EXISTS user_status'
user_status = user_status + ' (user_id int, session_id int, item_in_session int, artist text, song text, first text, last text, PRIMARY KEY(user_id, session_id, item_in_session))'
​
#execute user status query
executeQuery(session, user_status)
​
​
song_event = 'DROP TABLE IF EXISTS song_event'
# Clear Database for user song event and create tables
executeQuery(session, song_event)
​
song_event = 'CREATE TABLE IF NOT EXISTS song_event'
# Primary key is made up of song and user_id where the song is used as the partition key and user_id as a clustering column
song_event =  song_event + ' (song text, user_id int, first text, last text, PRIMARY KEY (song, user_id))'
executeQuery(session, song_event)
​
​
file = 'event_datafile_new.csv'
# music session insert query builder
music_session = 'INSERT INTO music_session (session_id, item_in_session, artist, song, length)'
music_session = music_session + ' VALUES (%s, %s, %s, %s, %s)'
​
# user status insert query builder
user_status = 'INSERT INTO user_status (user_id, session_id, item_in_session, artist, song, first, last)'
user_status = user_status + ' VALUES (%s, %s, %s, %s, %s, %s, %s)'
​
# song event insert query builder
song_event = 'INSERT INTO song_event (song, user_id, first, last)'
song_event = song_event + ' VALUES (%s, %s, %s, %s)'
​
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) 
    for line in csvreader:
        
        artist = line[0]
        first = line[1]
        item_in_session = int(line[3])
        last = line[4]
        length = float(line[5])
        session_id = int(line[8])
        song = line[9]
        user_id = int(line[10])
        
        # Insert into music_session
        session.execute(music_session, (session_id, item_in_session, artist, song, length))
              
        # Insert into user_status
        session.execute(user_status, (user_id, session_id, item_in_session, artist, song, first, last))
        
        # Insert into song_event
        session.execute(song_event, (song, user_id, first, last))
        
Do a SELECT to verify that the data have been inserted into each table
query = 'SELECT  artist, song, length from music_session WHERE session_id = 338 and item_in_session = 4'
session_df = None
try:
    rows = session.execute(query)
    session_df = retrieve_pandas(['artist', 'song', 'length'], rows)
except Exception as e:
    print(e)
​
session_df
artist	song	length
0	Faithless	Music Matters (Mark Knight Dub)	495.307312
COPY AND REPEAT THE ABOVE THREE CELLS FOR EACH OF THE THREE QUESTIONS
query = 'SELECT  artist, song, first, last from user_status WHERE user_id = 10 and session_id = 182'
user_status_df = None
try:
    rows = session.execute(query)
    user_status_df = retrieve_pandas(['artist', 'song', 'first', 'last'], rows)
except Exception as e:
    print(e)
user_status_df.head()                 
artist	song	first	last
0	Down To The Bone	Keep On Keepin' On	Sylvie	Cruz
1	Three Drives	Greece 2000	Sylvie	Cruz
2	Sebastien Tellier	Kilometer	Sylvie	Cruz
3	Lonnie Gordon	Catch You Baby (Steve Pitron & Max Sanna Radio...	Sylvie	Cruz
query = "SELECT first, last from song_event WHERE song = 'All Hands Against His Own' "
song_event_df = None
try:
    rows = session.execute(query)
    song_event_df = retrieve_pandas(['first', 'last'], rows)
except Exception as e:
    print(e)
    
song_event_df
                    
first	last
0	Jacqueline	Lynch
1	Tegan	Levine
2	Sara	Johnson
Drop the tables before closing out the sessions
​
query = "DROP TABLE IF EXISTS music_session"
try:
    session.execute(query)
except Exception as e:
    print(e)
​
query = "DROP TABLE IF EXISTS user_status"
try:
    session.execute(query)
except Exception as e:
    print(e)
​
query = "DROP TABLE IF EXISTS song_event"
try:
    session.execute(query)
except Exception as e:
    print(e)
​
Close the session and cluster connection¶
session.shutdown()
cluster.shutdown()