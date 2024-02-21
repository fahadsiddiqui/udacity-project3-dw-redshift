import logging

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

logging.basicConfig()
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)


def load_staging_tables(cur, conn):
    """
    Loads data from S3 into staging tables as per queries defined in sql_queries module.

    Args:
        cur: Cursor object to execute SQL queries.
        conn: Connection object to the Redshift database.

    Returns:
        None
    """
    for query in copy_table_queries:
        logger.info(f"Running QUERY:\n {query}")
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Transforms and copies data from staging tables into analytics tables.

    Args:
        cur: Cursor object to execute SQL queries.
        conn: Connection object to the Redshift database.

    Returns:
        None
    """
    for query in insert_table_queries:
        logger.info(f"Running QUERY:\n {query}")
        cur.execute(query)
        conn.commit()


def main():
    """
    Driver function connecting to Redshift and calling functions to load data into staging and analytics tables.

    Args:
        None

    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    logger.info("Connected to Redshift")

    logger.info("Loading staging tables")
    load_staging_tables(cur, conn)

    logger.info("Loading analytics tables")
    insert_tables(cur, conn)

    conn.close()
    logger.info("ETL completed successfully")


if __name__ == "__main__":
    main()
