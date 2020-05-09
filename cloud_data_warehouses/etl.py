import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

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
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()