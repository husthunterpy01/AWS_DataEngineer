import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from tqdm import tqdm  # Import tqdm for progress bars

def load_staging_tables(cur, conn):
    total_queries = len(copy_table_queries) 
    with tqdm(total=total_queries, desc="Loading staging tables", unit="query") as pbar:
        for query in copy_table_queries:
            cur.execute(query)
            conn.commit()
            pbar.update(1) 

def insert_tables(cur, conn):
    total_queries = len(insert_table_queries) 
    with tqdm(total=total_queries, desc="Inserting into tables", unit="query") as pbar:
        for query in insert_table_queries:
            cur.execute(query)
            conn.commit()
            pbar.update(1)  # Update progress bar by one step

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()
