%load_ext sql
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    drop tables to prevent tables from overlapping 
    with repeat implementations
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    create tables through queries in create_table_queries
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print("complete")


def main():
    """
    main function which processes the drop and creation of tables
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()