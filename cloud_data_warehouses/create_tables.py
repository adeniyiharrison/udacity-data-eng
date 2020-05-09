import configparser
import psycopg2
from sql_queries import (
    create_table_queries,
    drop_table_query
)


def drop_tables(cur, conn):
    for table in [
        "staging_events",
        "staging_songs",
        "songplays",
        "users",
        "songs",
        "artists",
        "timestamps"
    ]:
        cur.execute(query.format(
            table=table
            )
        )
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()