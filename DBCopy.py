import json
import multiprocessing
from sqlalchemy import create_engine
import pandas as pd

# source and destination databases
source_engine = create_engine('')
destination_engine = create_engine('')

def transfer_table(table_name):
    # try:
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
            # return
        # Get primary key information
        primary_key = None
        query = f"SELECT pg_attribute.attname FROM pg_constraint, pg_class, pg_attribute WHERE pg_constraint.conrelid = pg_class.oid AND pg_attribute.attrelid = pg_class.oid AND pg_attribute.attnum = any(pg_constraint.conkey) AND pg_constraint.contype = 'p' AND pg_class.relname = '{table_name}'"
        result = source_conn.execute(query)
        if primary_key_columns := [row[0] for row in result]:
            primary_key = f"{','.join(primary_key_columns)}"
            print(primary_key, 'PK')


        data.to_sql(table_name, dest_conn, if_exists='replace', index=False, index_label=primary_key, method="multi")

        dest_conn.close()
        source_conn.close()
    # except:
    #     print(table_name, 'EXCEPTION')

def add_unique(table_name):
    with source_engine.connect() as source_conn, destination_engine.connect() as dest_conn:
        # query = f"""
        #         SELECT column_name
        #         FROM information_schema.columns
        #         WHERE table_name = '{table_name}'
        #         AND is_nullable = 'NO'
        #         AND column_default IS NOT NULL
        #         AND column_default LIKE 'nextval%'
        # """
        query = text("""SELECT columns.column_name as name, constraints.constraint_name as constraint
                        FROM information_schema.columns
                        JOIN information_schema.table_constraints AS constraints
                        ON columns.table_name = constraints.table_name
                        WHERE columns.table_name = :table_name
                        AND columns.is_nullable = 'NO'
                        AND columns.column_default IS NOT NULL
                        AND columns.column_default LIKE 'nextval%'
                        AND constraints.constraint_type = 'UNIQUE';""")

        result = source_conn.execute(query, table_name=table_name)
        unique_cons = [{'name': row[0],'constraint': row[1],} for row in result]

        for row in unique_cons:
            dest_conn.execute(f"""
            ALTER TABLE {table_name}
            ADD CONSTRAINT {row['constraint']}
            UNIQUE ({row['name']});
    
            """)
    dest_conn.close()
    source_conn.close()

def add_fk(table_name):
    with source_engine.connect() as source_conn, destination_engine.connect() as dest_conn:

        foreign_keys = []
        query = f"""SELECT 
                        tc.constraint_name, 
                        tc.table_name, 
                        kcu.column_name, 
                        ccu.table_name AS referenced_table, 
                        ccu.column_name AS referenced_column
                    FROM 
                        information_schema.table_constraints AS tc 
                        JOIN information_schema.key_column_usage AS kcu 
                            ON tc.constraint_name = kcu.constraint_name 
                        JOIN information_schema.constraint_column_usage AS ccu 
                            ON ccu.constraint_name = tc.constraint_name
                    WHERE 
                        tc.constraint_type = 'FOREIGN KEY' 
                        AND tc.table_name = '{table_name}'
                    ORDER BY 
                        tc.constraint_name, kcu.ordinal_position;"""

        result = source_conn.execute(query)
        for row in result:
            foreign_keys.append({
                'name': row[0],
                'referenced_table': row[3],
                'columns': row[2],
                'referenced_columns': row[4]
            })
        for foreign_key in foreign_keys:
            print(foreign_key)
            try:
                dest_conn.execute(
                    f"ALTER TABLE {table_name} ADD CONSTRAINT {foreign_key['name']} FOREIGN KEY ({foreign_key['columns']}) REFERENCES {foreign_key['referenced_table']}({foreign_key['referenced_columns']}) ")
                print(foreign_key,' ADDED')
            except:
                print(foreign_key,'GOT AN ISSUE')
        dest_conn.close()
        source_conn.close()


if __name__ == '__main__':
    with source_engine.connect() as conn:
        table_names = [table_name[0] for table_name in conn.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")]
    table_names = [s for s in table_names if not any(i.isdigit() for i in s)]

    # Create a pool of worker processes
    pool = multiprocessing.Pool(processes=4)

    # Start the worker processes
    pool.map(transfer_table, table_names)
    pool.map(add_unique, table_names)
    # add_unique(table_names[0])
    pool.map(add_fk, table_names)


    # Close the pool
    pool.close()
    pool.join()
    with destination_engine.connect() as dest_conn:
        dest_conn.execute(
            "DO $$ DECLARE table_rec record; BEGIN FOR table_rec IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP EXECUTE 'ALTER TABLE ' || table_rec.tablename || ' DROP COLUMN IF EXISTS \"index\"'; END LOOP; END $$;")
    dest_conn.close()
    conn.close()
