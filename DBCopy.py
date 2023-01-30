import json
import multiprocessing
from sqlalchemy import create_engine
import pandas as pd

# source and destination databases
source_engine = create_engine('')
destination_engine = create_engine('')

def transfer_table(table_name):
    print(table_name)
    with source_engine.connect() as source_conn, destination_engine.connect() as dest_conn:
        query = f"SELECT * FROM {table_name} LIMIT 1000"
        data = pd.read_sql_query(query, source_conn)
        if not data.empty:
            if 'index' in data.columns:
                data.drop('index', axis=1, inplace=True)
            #Checking for columns with dict data type
            for col in data.columns:
                if data[col].dtype == 'object' and isinstance(data[col].iloc[0], dict):
                    data[col] = data[col].apply(lambda x: json.dumps(x))
        else:
            print(f"{table_name} is empty")
            return
        data.to_sql(table_name, dest_conn, if_exists='replace')

if __name__ == '__main__':

    with source_engine.connect() as conn:
        table_names = [table_name[0] for table_name in conn.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")]
    table_names = [s for s in table_names if not any(i.isdigit() for i in s)]
    # Create a pool of worker processes
    pool = multiprocessing.Pool(processes=4)

    # Start the worker processes
    pool.map(transfer_table, table_names)

    # Close the pool
    pool.close()
    pool.join()
    with destination_engine.connect() as dest_conn:
        dest_conn.execute(
            "DO $$ DECLARE table_rec record; BEGIN FOR table_rec IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP EXECUTE 'ALTER TABLE ' || table_rec.tablename || ' DROP COLUMN IF EXISTS \"index\"'; END LOOP; END $$;")
