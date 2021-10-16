import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from time import time


def load_staging_tables(cur, conn):
    """Copies data from json files in a S3 datastore to staging tables on a Redshift cluster"""
    print('*******LOADING STAGING TABLES*******')
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Extracts Transforms and Loads data present in the Redshift staging tables to the star schema tables on Redshift"""
    print('*******LOADING INSERT TABLES*******')
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Accesses my Redshift cluster and runs the load staging tables and insert tables functions"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()