import logging
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

logging.basicConfig()
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)


def drop_tables(cur, conn):
    """
    Drops tables if they exist as per the queriØs listed in the sql_queries module.

    Args:
        cur: The cursor object for executing SQL queries.
        conn: The connection object to the Redshift database.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates tables as per the queries listed in the sql_queries module.

    Args:
        cur: The cursor object for executing SQL queries.
        conn: The connection object to the Redshift database.
    """
    for query in create_table_queries:
        logger.info(f"Running QUERY:\n {query}")
        cur.execute(query)
        conn.commit()


def main():
    """
    Connects to Redshift, drops tables if they exist, and then creates new tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    logger.info("Connected to Redshift")

    drop_tables(cur, conn)
    logger.info("Tables dropped")

    create_tables(cur, conn)
    logger.info("Tables created")

    conn.close()
    logger.info("Tables created successfully")


if __name__ == "__main__":
    main()
