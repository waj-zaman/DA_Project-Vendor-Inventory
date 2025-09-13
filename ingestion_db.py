import duckdb
import time
import logging
import os

database_path = "/vendor_inventory.duckdb"
dataset_path = "data"
log_path = "log/ingestion_db.log"

conn = duckdb.connect(database_path)

# Configure the logging file
logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a'
)

# Actual Ingestion code
def ingest_db(table_name, csv_file, conn):
    '''This function ingests the data into the database'''
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM read_csv_auto('{csv_file}');")

# Broader Ingestion function
def load_raw_data_duckdb():
  '''This function is used to ingest all the data directly into db without pandas'''
  start_time = time.time()

  for file in os.listdir(dataset_path):
    if file.endswith(".csv"):
      file_start = time.time()

      table_name = file[:-4]
      csv_file = f"{dataset_path}/{file}"
      ingest_db(table_name, csv_file, conn)  
        
      file_end = time.time()
      logging.info(f"Ingested {file} into table {table_name} in {file_end - file_start}")
      print (f"Ingested {file} into {table_name} in vendor_inventory.duckdb")

  end_time = time.time()
  logging.info(f"Total time taken by ingestion process: {end_time - start_time}")
  print (f"Total time taken by ingestion process: {end_time - start_time}")

if __name__ == "__main__":
  load_raw_data_duckdb()