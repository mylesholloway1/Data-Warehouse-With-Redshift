import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS event_stage"
staging_songs_table_drop = "DROP TABLE IF EXISTS song_stage"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# CREATE TABLES

staging_songs_table_create= ("""CREATE TABLE IF NOT EXISTS song_stage \
                                (num_songs int, \
                                artist_id varchar, \
                                artist_latitude varchar, \
                                artist_longitude varchar, \
                                artist_location varchar, \
                                artist_name varchar, \
                                song_name varchar, \
                                song_id varchar, \
                                title varchar, \
                                duration float, \
                                year int)
""")

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS event_stage \
                                (artist_name varchar, \
                                auth varchar, \
                                first_name varchar, \
                                gender varchar, \
                                item_in_session int, \
                                last_name varchar, \
                                length float, \
                                level varchar, \
                                location varchar, \
                                method varchar, \
                                page varchar, \
                                registration float, \
                                session_id int, \
                                song varchar, \
                                status int, \
                                ts varchar, \
                                user_agent varchar, \
                                user_id int);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays \
                            (songplay_id bigint IDENTITY(0,1) PRIMARY KEY, \
                            start_time varchar, \
                            user_id int NOT NULL, \
                            level varchar, \
                            song_id varchar NOT NULL, \
                            artist_id varchar NOT NULL, \
                            session_id int, \
                            location varchar, \
                            user_agent varchar);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users \
                        (user_id int PRIMARY KEY, \
                        first_name varchar, \
                        last_name varchar, \
                        gender varchar, \
                        level varchar);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs \
                        (song_id varchar PRIMARY KEY, \
                        title varchar, \
                        artist_id varchar NOT NULL, \
                        year int, \
                        duration numeric);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists \
                          (artist_id varchar PRIMARY KEY, \
                          name varchar, \
                          location varchar, \
                          latitude numeric, \
                          longitude numeric);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time \
                        (start_time varchar PRIMARY KEY, \
                        hour int, \
                        day int, \
                        week int, \
                        month int, \
                        year int, \
                        weekday int);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY event_stage FROM 's3://udacity-dend/log_data'
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS JSON 's3://udacity-dend/log_json_path.json' REGION 'us-west-2'
""").format(*config['IAM_ROLE'].values())

staging_songs_copy = ("""
    COPY song_stage FROM 's3://udacity-dend/song_data'
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS JSON 'auto' REGION 'us-west-2'
""").format(*config['IAM_ROLE'].values())

# FINAL TABLES

songplay_table_insert = ("""
    
    INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
    ) 
    SELECT e.ts as start_time,
        e.user_id as user_id,
        e.level as level,
        s.song_id as song_id,
        s.artist_id as artist_id,
        e.session_id as session_id,
        e.location as location,
        e.user_agent as user_agent
    FROM event_stage e
    JOIN song_stage s ON (e.song = s.title AND e.artist_name = s.artist_name) 
""")

user_table_insert = ("""
    INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level
    )
    SELECT DISTINCT e.user_id as user_id,
        e.first_name as first_name,
        e.last_name as last_name,
        e.gender as gender,
        e.level as level
    FROM event_stage e
    WHERE user_id IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration
    )
    SELECT DISTINCT s.song_id as song_id,
        s.title as title,
        s.artist_id as artist_id,
        s.year as year,
        s.duration as duration
    FROM song_stage s
""")

artist_table_insert = ("""
    INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude
    )
    SELECT DISTINCT s.artist_id as artist_id,
        s.artist_name as name,
        s.artist_location as location,
        s.artist_latitude as latitude,
        s.artist_longitude as longitude
    FROM song_stage s
""")


time_table_insert = ("""
    INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
    )
    SELECT DISTINCT timestamp 'epoch' + CAST(e.ts AS BIGINT)/1000 * interval '1 second' as start_time,
        extract(HOUR FROM timestamp 'epoch' + CAST(e.ts AS BIGINT)/1000 * interval '1 second') as hour,
        extract(DAY FROM timestamp 'epoch' + CAST(e.ts AS BIGINT)/1000 * interval '1 second') as day,
        extract(WEEK FROM timestamp 'epoch' + CAST(e.ts AS BIGINT)/1000 * interval '1 second') as week,
        extract(MONTH FROM timestamp 'epoch' + CAST(e.ts AS BIGINT)/1000 * interval '1 second') as month,
        extract(YEAR FROM timestamp 'epoch' + CAST(e.ts AS BIGINT)/1000 * interval '1 second') as year,
        extract(DAY FROM timestamp 'epoch' + CAST(e.ts AS BIGINT)/1000 * interval '1 second') as weekday
    FROM event_stage e
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
