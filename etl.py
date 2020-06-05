import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    loading data from s3 bucket into staging tables
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    moving data from staging tables to main tables
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
    print("complete")


def main():
    """
    main fuction to process data
    """
    config = configparser.ConfigParser()
    
    config.read_file(open('dwh.cfg'))
    KEY=config.get('AWS','key')
    SECRET= config.get('AWS','secret')

    DWH_DB= config.get("DWH","DWH_DB")
    DWH_DB_USER= config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD= config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT = config.get("DWH","DWH_PORT")
     
    DWH_ENDPOINT=config.get("DWH", "DWH_ENDPOINT") 
    DWH_ROLE_ARN=config.get("IAM_ROLE", "ARN")

    conn = psycopg2.connect("postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()