import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from tqdm import tqdm

def drop_tables(cur, conn):
    print("Starting table drop...")
    for query in tqdm(drop_table_queries, desc="Dropping tables", unit="table"):
        cur.execute(query)
        conn.commit()
    print("All tables dropped successfully.")


def create_tables(cur, conn):
    print("Starting table creation...")
    total_tables = len(create_table_queries)
    
    for query in tqdm(create_table_queries, desc="Creating tables", unit="table"):
        cur.execute(query)
        conn.commit()
    
    print("All tables created successfully.")
    
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