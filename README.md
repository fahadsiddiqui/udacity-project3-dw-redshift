# Data Warehouse with Amazon Redshift

## Project Overview

A music streaming startup, Sparkify, has seen their user base and song database expand and is looking to shift their processes and data onto the cloud. Their data is stored in S3, in a directory of JSON logs detailing user activity on the app, along with a directory containing JSON metadata on the songs in their app.

## Project Description

This project develops an ETL pipeline that extracts Sparkify's data from S3, stages it in Redshift, and transforms the data into a set of dimensional tables for their analytics team to glean insights on what songs users are playing.

## Run Guide

- Install pipenv via pip
- Run `pipenv install`
- Run `pipenv shell` to activate the virtual environment
- Edit `dwh.cfg` with your Redshift cluster's details and an IAM role that can access S3
- Execute the `create_tables.py` script to create tables in the Redshift database as configured in `dwh.cfg`
- Run the `etl.py` script to fill all tables

## Data Model

The data model splits into two sets of tables. Staging tables load raw data into Redshift. These tables are then used to fill analytics tables after applying SQL transformations.

### Staging Tables

- **staging_log_events**: Pulls log events data from `s3://udacity-dend/log_data` using the `COPY` command. Uses the `song` column as `distkey`

- **staging_songs**: Pulls songs data from `s3://udacity-dend/log_data` using the `COPY` command. The `title` column, which is the song title, is used as `distkey`

Both data sources are in JSON format.

### Analytics Tables (Star Schema)

- **fact_songplays**: The schema's fact table. `song_id` is used as `distkey` for distributing data across slices. The `dim_songs` table also uses `song_id` as `distkey`, minimizing data shuffling when joining these tables. Constraints are added in the table creation statement

- **dim_songs**: The songs dimensional table uses `song_id` as both `distkey` and `sortkey`

- **dim_artists**: The artists dimensional table uses `diststyle all` and sorts on `artist_id`

- **dim_users**: The users dimensional table uses `diststyle all` and sorts on `user_id`

- **dim_time**: The time dimensional table uses `diststyle all` and sorts on `start_time`

## Description of Project Files

- **create_tables.py**: Drops tables if they exist and then creates tables using queries defined in the `sql_queries.py` module

- **dwh.cfg**: Holds the Redshift database connection parameters, IAM role for the `COPY` command to access S3 data files, and paths to S3 data files

- **etl.py**: Executes queries defined in the `sql_queries.py` module. It first populates staging tables and then transfers data to analytics tables by applying SQL transformations

- **sql_queries.py**: Contains queries for creating tables, copying data from S3, and then transforming and inserting data into analytics tables

## Author

- [Fahad Siddiqui](https://github.com/fahadsiddiqui)
