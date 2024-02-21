import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE_ARN = config.get("IAM_ROLE", "ARN")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
LOG_DATA = config.get("S3", "LOG_DATA")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_log_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays;"
user_table_drop = "DROP TABLE IF EXISTS dim_users;"
song_table_drop = "DROP TABLE IF EXISTS dim_songs;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE staging_log_events (
        artist              VARCHAR(1024),
        auth                VARCHAR(20),
        firstName           VARCHAR(250),
        gender              CHAR(1),
        itemInSession       INTEGER,
        lastName            VARCHAR(250),
        length              FLOAT,
        level               VARCHAR(100),
        location            VARCHAR(1024),
        method              VARCHAR(15),
        page                VARCHAR(50),
        registration        FLOAT,
        sessionId           INT,
        song                VARCHAR(250) DISTKEY,
        status              SMALLINT,
        ts                  BIGINT,
        userAgent           TEXT,
        userId              INT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        artist_id           VARCHAR(100),
        artist_name         VARCHAR(1024),
        artist_location     VARCHAR(1024),
        artist_latitude     VARCHAR(100),
        artist_longitude    VARCHAR(100),
        song_id             VARCHAR(100),
        title               VARCHAR(1024) DISTKEY,
        duration            FLOAT,
        year                SMALLINT
    );
""")

songplay_table_create = ("""
    CREATE TABLE fact_songplays (
        songplay_id         BIGINT IDENTITY(0, 1) PRIMARY KEY,
        start_time          BIGINT REFERENCES dim_time(start_time) SORTKEY,
        user_id             INT REFERENCES dim_users(user_id),
        level               VARCHAR(100),
        song_id             VARCHAR(30) REFERENCES dim_songs(song_id) DISTKEY,
        artist_id           VARCHAR(30) REFERENCES dim_artists(artist_id),
        session_id          INT NOT NULL,
        location            VARCHAR(1024),
        user_agent          TEXT
    );
""")

user_table_create = ("""
    CREATE TABLE dim_users (
        user_id             BIGINT PRIMARY KEY SORTKEY,
        first_name          VARCHAR(250) NOT NULL,
        last_name           VARCHAR(250),
        gender              CHAR(1) NOT NULL,
        level               VARCHAR(100) NOT NULL
    )
    DISTSTYLE ALL;
""")

song_table_create = ("""
    CREATE TABLE dim_songs (
        song_id             VARCHAR(100) PRIMARY KEY SORTKEY DISTKEY,
        title               VARCHAR(1024) NOT NULL,
        artist_id           VARCHAR NOT NULL REFERENCES dim_artists(artist_id),
        year                SMALLINT,
        duration            FLOAT NOT NULL DEFAULT 0
    );
""")

artist_table_create = ("""
    CREATE TABLE dim_artists (
        artist_id           VARCHAR PRIMARY KEY SORTKEY,
        name                VARCHAR(1024) NOT NULL,
        location            VARCHAR(1024),
        latitude            VARCHAR(100),
        longitude           VARCHAR(100)
    )
    DISTSTYLE ALL;
""")

time_table_create = ("""
    CREATE TABLE dim_time (
        start_time          BIGINT PRIMARY KEY SORTKEY,
        hour                SMALLINT NOT NULL,
        day                 SMALLINT NOT NULL,
        week                SMALLINT NOT NULL,
        month               SMALLINT NOT NULL,
        year                SMALLINT NOT NULL,
        weekday             VARCHAR(15) NOT NULL
    )
    DISTSTYLE ALL;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_log_events 
    FROM {log_data}
    CREDENTIALS 'aws_iam_role={iam_role_arn}'
    REGION 'us-west-2'
    FORMAT AS JSON {log_jsonpath};
""").format(log_data=LOG_DATA, iam_role_arn=IAM_ROLE_ARN, log_jsonpath=LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs
    FROM {songs_data}
    CREDENTIALS 'aws_iam_role={iam_role_arn}'
    REGION 'us-west-2'
    JSON 'auto';
""").format(songs_data=SONG_DATA, iam_role_arn=IAM_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO fact_songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT 
            e.ts,
            e.userId,
            e.level,
            s.song_id,
            s.artist_id,
            e.sessionId,
            e.location,
            e.userAgent
        FROM staging_log_events e
        LEFT JOIN staging_songs s 
            ON e.song = s.title AND e.artist = s.artist_name
        WHERE page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO dim_users
        SELECT user_id, first_name, last_name, gender, level
        FROM (
            SELECT DISTINCT userId as user_id,
                firstname as first_name,
                lastname as last_name,
                gender,
                level
            FROM staging_log_events
            WHERE user_id IS NOT NULL
            ORDER BY user_id
        ) AS tmp
""")

song_table_insert = ("""
    INSERT INTO dim_songs (song_id, title, artist_id, year, duration)
        SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO dim_artists (artist_id, name, location, latitude, longitude)
        SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO dim_time
        SELECT start_time,
            date_part(hour, date_time) AS hour,
            date_part(day, date_time) AS day,
            date_part(week, date_time) AS week,
            date_part(month, date_time) AS month,
            date_part(year, date_time) AS year,
            date_part(weekday, date_time) AS weekday
        FROM (SELECT ts AS start_time,
            '1970-01-01'::date + ts/1000 * interval '1 second' AS date_time
            FROM staging_log_events
            GROUP BY ts) AS tmp
        ORDER BY start_time;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
