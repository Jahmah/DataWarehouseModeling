# Background Information 
The music streaming startup, Sparkify, has grown their user base and song database and has hired me to move their data onto the cloud. Currently, their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. This will allow Sparkify analytics team to query and analyze thier data efficiently in the cloud. 


# Solution 
I have built an ETL pipeline that 
- extracts their data from S3 
- stages the files in Redshift
- transforms data into a set of dimensional tables 

This will allow their analytics team to continue finding insights in what songs their users are listening to. 


## How to run 
- Update the config file (`dl.cfg`) with your AWS credentails and Redshift cluster details
- Run `python create_tables.py` from this root directory to create the tables
- Run `python etl.py` to load the staging tables and insert data into tables


### Files
- `create_tables.py` - python file to create the databese and create and drop tables.
- `sql_queries.py` - python file with queries to drop, create, copy and insert these tables.
- `etl.py` - ETL pipeline to extract, transform and load the data to our tables. 
- `dwh.cfg` - AWS Redshift's parameters argument file.


## Dependencies
- `psycopg2`
- `configparser`


## Additional Design Info 

### Fact and Dimension Tables

Facts and dimensions form the core of any business intelligence effort. These tables contain the basic data used to conduct detailed analyses and derive business value. 
- A fact table consists of the measurements, metrics or facts of a business process.  Events that have actually happened. 
- A dimension table is a structure that categorizes facts and measures in order to enable users to answer business questions. Commonly used dimensions are people, products, place and time.

### Star Schema
Here I have developed a star schema for Sparkify. The star schema separates business process data into facts, which hold the measurable, quantitative data about a business, and dimensions which are descriptive attributes related to fact data. A star schema allows for simpler queries, fast aggregations and the ability to denormalize your data. 


### Fact Table
**songplays** - records in log data associated with song plays i.e. records with page NextSong
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
**users** - users in the app
- user_id, first_name, last_name, gender, level

**songs** - songs in music database
- song_id, title, artist_id, year, duration

**artists** - artists in music database
- artist_id, name, location, lattitude, longitude

**time** - timestamps of records in songplays broken down into specific units
- start_time, hour, day, week, month, year, weekday


### Stagging Tables
**staging_events** - log data files
- event_id, artist, auth, first_name, gender, item_session, last_name, length, level, location, method, page, registration, session_id, song, status, ts, user_agent, user_id

**staging_songs** - song metadata data files 
- num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year


## Example Query

### Query 1

Find me the percentage of 'paid' accounts for males and females 
```
SELECT 
    COUNT(*) GENDER_COUNT,
    gender, 
    ROUND(AVG(CASE WHEN level = 'paid' THEN 1 ELSE 0 END), 2) PAID_PERCENT
FROM user_data
GROUP BY 2;
```
