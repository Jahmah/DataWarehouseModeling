import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load staging tables. It performs a copy from `sql_queries` file from `copy_table_queries` variable
    Args:
        cur (object): psycopg2 cursor for a database connection
        conn (object): connection instance from psycopg2 cluster
    Returns:
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert rows into tables from `sql_queries` file from `insert_table_queries` variable
    Args:
        cur (object): psycopg2 cursor for a database connection
        conn (object): connection instance from psycopg2 cluster
    Returns:
        None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main Function to 
    - read in configuration details
    - connect to RedShift Cluster
    - loads staging tables
    - inserts data to all tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    # connection
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    try:
        load_staging_tables(cur, conn)
        print('Loaded Staging Tables')
    except Exception as e:
        print(str(e))
        
    try:
        insert_tables(cur, conn)
        print('Inserted Data')
    except Exception as e:
        print(str(e))
    
    # close connection
    conn.close()
    
    


if __name__ == "__main__":
    main()