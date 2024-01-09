import os
import json
import pandas as pd
import mysql.connector

CURRENT_FILE_PATH = os.path.abspath(__file__)
CURRENT_DIRECTORY = os.path.dirname(CURRENT_FILE_PATH)
EXTRA_DB_CONFIG_PATH = f"%s/../../extra_db_config.json"%(CURRENT_DIRECTORY)

def load_db_config(file_path):
    with open(file_path, 'r') as config_file:
        db_config = json.load(config_file)
    return db_config

def drop_and_create_database(
    conn, 
    database_name:str, 
    drop_flag:bool = True
    ):
    try:
        connector_conn = mysql.connector.connect(**conn)
        connector_cursor = connector_conn.cursor()

        check_query = f"SHOW DATABASES LIKE '%s';"%(database_name)

        connector_cursor.execute(check_query)
        qeury_result = connector_cursor.fetchall()

        if qeury_result:
            print(f"[CODE::COMPLETE] Database '%s' Exists."%(database_name))
            
            if drop_flag:
                drop_query = f"DROP DATABASE %s;"%(database_name)
                connector_cursor.execute(drop_query)
                print(f"[CODE::COMPLETE] Database '%s' Deleted."%(database_name))
        else:
            print(f"[CODE::COMPLETE] Database '%s' NOT exists."%(database_name))

        create_query = f"CREATE DATABASE %s;"%(database_name)
        connector_cursor.execute(create_query)
        print(f"[CODE::COMPLETE] Database '%s' is Created."%(database_name))

    except Exception as e:
        print(f"[CODE::ERROR] {e}")

    finally:
        connector_cursor.close()
        connector_conn.close()

def drop_and_create_table(
    conn, 
    table_name:str, 
    table_feature:str,
    drop_flag:bool = True
    ):
    try:
        connector_conn = mysql.connector.connect(**conn)
        connector_cursor = connector_conn.cursor()

        check_query = f"SHOW TABLES LIKE '%s';"%(table_name)

        connector_cursor.execute(check_query)
        qeury_result = connector_cursor.fetchall()

        if qeury_result:
            print(f"[CODE::COMPLETE] Table '%s' Exists."%(table_name))
            
            if drop_flag:
                drop_query = f"DROP TABLE %s;"%(table_name)
                connector_cursor.execute(drop_query)
                print(f"[CODE::COMPLETE] Table %s Deleted."%(table_name))
        else:
            print(f"[CODE::COMPLETE] Table %s NOT exists."%(table_name))

        create_query = f"CREATE TABLE %s (%s);"%(table_name, table_feature)
        connector_cursor.execute(create_query)
        print(f"[CODE::COMPLETE] Table %s is Created."%(table_name))

    except Exception as e:
        print(f"[CODE::ERROR] {e}")

    finally:
        connector_cursor.close()
        connector_conn.close()

def list_to_comma_seperated_str(insert_list:list, use_quotation:bool):
    result_str = f""
    for insert_value in insert_list:
        if len(result_str) == 0:
            if use_quotation:
                result_str += f"\"%s\""%(str(insert_value).replace("\"", r"\'"))
            else:
                result_str += f"%s"%(str(insert_value).replace("\"", r"\'"))
        else:
            if use_quotation:
                result_str += f",\"%s\""%(str(insert_value).replace("\"", r"\'"))
            else:
                result_str += f",%s"%(str(insert_value).replace("\"", r"\'"))
    return result_str

def list_to_insert_query(
    insert_table:str, 
    insert_feature_list:list, 
    insert_value_list:list
    ):
    return f"INSERT INTO %s (%s) VALUES (%s);"%(
        insert_table, 
        list_to_comma_seperated_str(insert_feature_list, use_quotation=False), 
        list_to_comma_seperated_str(insert_value_list, use_quotation=True)
        )

def insert_txt_as_sql(
    file_path, 
    conn, 
    table_name:str, 
    table_features:list
    ):
    try:
        connector_conn = mysql.connector.connect(**conn)
        connector_cursor = connector_conn.cursor()

        with open(file_path, 'r') as file:
            insert_lines = file.readlines()

            for insert_line in insert_lines:
                if insert_line[0] == "#":
                    continue
                insert_data = insert_line.strip().split('\t')
                insert_query = list_to_insert_query(table_name, table_features, insert_data)
                print(insert_query)
                connector_cursor.execute(insert_query)

        connector_conn.commit()
        print("[CODE::COMPLETED] Successfully Inserted.")

    except Exception as e:
        print(f"[CODE::ERROR] {e}")

    finally:
        connector_cursor.close()
        connector_conn.close()

def insert_sql_chembl_uniprot_mapping_txt():
    chembl_uniprot_maping_txt_loc = f"%s/%s"%(
        CURRENT_DIRECTORY, 
        "../data/external_data/chembl_uniprot_mapping.txt"
        )
    db_config = load_db_config(EXTRA_DB_CONFIG_PATH)
    db_feature_desc = ["uniprot_id", "chembl_id", "pref_name", "type"]

    drop_and_create_database(db_config, "extra_db")
    drop_and_create_table(db_config, "chembl_uniprot_mapping", "uniprot_id VARCHAR(255), chembl_id VARCHAR(255), pref_name VARCHAR(255), type VARCHAR(255)")

    insert_txt_as_sql(
        chembl_uniprot_maping_txt_loc, 
        db_config, 
        "chembl_uniprot_mapping", 
        db_feature_desc)

if __name__ == "__main__":
    insert_sql_chembl_uniprot_mapping_txt()
    # print(list_to_comma_seperated_str([1, 1, 1]))
    # print(list_to_insert_query("test", ["a", "b", "i"], [1, 1, 1]))
    
    
