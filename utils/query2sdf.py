import os
import json
import pandas as pd
import mysql.connector

from rdkit import Chem
from rdkit.Chem import PandasTools


CURRENT_FILE_PATH = os.path.abspath(__file__)
CURRENT_DIRECTORY = os.path.dirname(CURRENT_FILE_PATH)
EXTRA_DB_CONFIG_PATH = f"%s/../../chembl_33_config.json"%(CURRENT_DIRECTORY)

def load_db_config(file_path):
    with open(file_path, 'r') as config_file:
        db_config = json.load(config_file)
    return db_config


def tid_to_query(
    in_tid:str
    ):
    return f"""select md.chembl_id, cs.molfile, cp.ALOGP, cp.HBA, cp.HBD, cp.PSA
        from assays as ass, activities as act, molecule_dictionary as md, compound_structures as cs, compound_properties as cp
        where 
            ass.tid = %s 
            and ass.assay_id = act.assay_id
            and act.molregno = md.molregno 
            and act.molregno = cs.molregno
            and act.molregno = cp.molregno
        ;"""%(
        in_tid
        )

def query_to_df(
    conn, 
    query_string:str
    ) -> pd.DataFrame:
    try:
        connector_conn = mysql.connector.connect(**conn)
        connector_cursor = connector_conn.cursor()
        connector_cursor.execute(query_string)
        query_results = connector_cursor.fetchall()

        result_df = pd.DataFrame(query_results, columns=['chembl_id', 'molfile', 'ALOGP', 'HBA', 'HBD', 'PSA'])
        print(f"[CODE::COMPLETED] Successfully Queried. Reseult Shape: %s"%(str(result_df.shape)))
        return result_df

    except mysql.connector.Error as me:
        print(f"[MYSQL:: ERROR] {me}")

    except Exception as e:
        print(f"[CODE::ERROR] {e}")
    
def save_sdf(in_df, mol_column, id_column, properties, sdf_filename):
    with Chem.SDWriter(sdf_filename) as writer:
        for i, row in in_df.iterrows():

            mol = Chem.MolFromMolBlock(row[mol_column])
            mol.SetProp("_Name", id_column)
            for _prop in properties:
                mol.SetProp(_prop, str(row[_prop]))
            writer.write(writer, mol, i)



def query_to_sdf():
    test_tid = 104811 #bcr-abl fusion protein
    test_output_file = f"%s/../data/test_sdf_output.sdf"%CURRENT_DIRECTORY

    db_config = load_db_config(EXTRA_DB_CONFIG_PATH)
    test_query_string = tid_to_query(test_tid)
    result_df = query_to_df(db_config, test_query_string)
    save_sdf(result_df, mol_column='molfile', id_column='chembl_id', properties=["ALOGP", "HBA", "HBD", "PSA"], sdf_filename=test_output_file)





if __name__ == "__main__":
    query_to_sdf()
    
