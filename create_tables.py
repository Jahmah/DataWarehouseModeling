import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops all tables from `sql_queries` file from `drop_table_queries`
    Args:
        cur (object): psycopg2 cursor for a database connection
        conn (object): connection instance from psycopg2 cluster
    Returns:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates all tables from `sql_queries` file from `create_table_queries`
    Args:
        cur (object): psycopg2 cursor for a database connection
        conn (object): connection instance from psycopg2 cluster
    Returns:
        None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main Function to 
    - read in configuration details
    - connect to RedShift Cluster
    - drop tables if exist
    - create tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    # connection
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    try:
        drop_tables(cur, conn)
        print('Tables dropped')
    except Exception as e:
        print(str(e))
        print('No tables to drop')
        
    try:
        create_tables(cur, conn)
        print('Tables created')
    except Exception as e:
        print(str(e))
        print('No tables have been created')
    
    # close connection
    conn.close()


if __name__ == "__main__":
    main()