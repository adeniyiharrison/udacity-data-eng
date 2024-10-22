import configparser
import psycopg2
from sql_queries import (
    create_table_queries,
    drop_table_query
)


def drop_tables(cur, conn):
    """
        Drop all tables in cluster

        Aguements:
            cur: psycopg2 cursor
            conn: psycopg2 connection
            filepath: Filepath to song or log file
            func: Function to be used for processing
    """

    for table in [
        "staging_events",
        "staging_songs",
        "songplays",
        "users",
        "songs",
        "artists",
        "timestamps"
    ]:
        cur.execute(drop_table_query.format(
            table=table
            )
        )
        conn.commit()


def create_tables(cur, conn):
    """
        Create all required table for ETL

        Aguements:
            cur: psycopg2 cursor
            conn: psycopg2 connection
            filepath: Filepath to song or log file
            func: Function to be used for processing
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        Create pyscopg2 objects to run create_table process
    """
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    conn = psycopg2.connect(
        "host={host} dbname={dbname} user={user} password={password} port={port}".format(
            host=config.get("DWH", "DWH_ENDPOINT"),
            dbname=config.get("DWH", "DWH_DB"),
            user=config.get("DWH", "DWH_DB_USER"),
            password=config.get("DWH", "DWH_DB_PASSWORD"),
            port=config.get("DWH", "DWH_PORT")
        )
    )
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()