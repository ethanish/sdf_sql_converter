import os
import json
import pandas as pd

CURRENT_FILE_PATH = os.path.abspath(__file__)
CURRENT_DIRECTORY = os.path.dirname(CURRENT_FILE_PATH)
DB_CONFIG_PATH = f"%s/../data/external_data/chembl_uniprot_mapping.txt"%(CURRENT_DIRECTORY)

def load_db_config(file_path):
    with open(file_path, 'r') as config_file:
        db_config = json.load(config_file)
    return db_config

def is_exists_db_table():
    print()

def create_db_table(db_existance_flag:bool):
    print()

def insert_txt_as_sql(file_path, conn):
    try:
        connector_conn = mysql.connector.connect(**conn)
        connector_cursor = mysql_conn.cursor()

        with open(file_path, 'r') as file:
            insert_lines = file.readlines()

            for insert_line in insert_lines:
                if insert_line[0] == "#":
                    continue
                # TODO: clear here
                insert_data = insert_line.strip().split(',')
                insert_query = f"INSERT INTO your_table_name (column1, column2, column3) VALUES ('{insert_data[0]}', '{insert_data[1]}', '{insert_data[2]}');"

                # connector_cursor.execute(insert_query)
                print(insert_query)

        connector_conn.commit()

        print("[COMPLETED] Successfully Inserted.")

    except Exception as e:
        print(f"[ERROR] {e}")

    finally:
        connector_cursor.close()
        connector_conn.close()

def insert_sql_chembl_uniprot_mapping_txt():
    db_config = load_db_config(DB_CONFIG_PATH)
    # insert_table_name = "table_name"
    # if not is_exists_db_table(insert_table_name):
    #     create_db_table(True, db_config, insert_table_name)
    
    chembl_uniprot_maping_txt_loc = f"%s/%s"%(CURRENT_DIRECTORY, "../external_data/chembl_uniprot_mapping.txt")
    insert_txt_as_sql(chembl_uniprot_maping_txt_loc, db_config)

if __name__ == "__main__":
    insert_sql_chembl_uniprot_mapping_txt()
    
