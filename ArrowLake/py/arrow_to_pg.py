import psycopg2
import pyarrow as pa
from pyarrow.fs import GcsFileSystem
import pyarrow.feather as feather
from pgpq import ArrowToPostgresBinaryEncoder
from concurrent.futures import ThreadPoolExecutor
import time
import tempfile

def load_batch_to_temp_table(batch, encoder, cursor):
   with cursor.copy(f"COPY temp_data FROM STDIN WITH (FORMAT BINARY)") as copy:
       copy.write(encoder.write_batch(batch))

def load_feather_to_table(bucket_name, file_name, table_name, conn, batch_size=10000, max_workers=4):
   temp_table_name = f"temp_{table_name}"  # Define temp_table_name here

   try:
       # Download the Feather file to a local temporary file
       with GcsFileSystem().open_input_file(f"{bucket_name}/{file_name}") as gcs_file:
           with tempfile.NamedTemporaryFile(delete=False) as local_file:
               local_file.write(gcs_file.read())
               local_file_path = local_file.name

       # Read the schema from the Feather file
       table = feather.read_table(local_file_path)
       schema = table.schema

       # Create an encoder object
       encoder = ArrowToPostgresBinaryEncoder(schema)
       pg_schema = encoder.schema()

       # Assemble DDL for a temporary table
       cols = [f'"{col_name}" {col.data_type.ddl()}' for col_name, col in pg_schema.columns]
       ddl = f"CREATE TEMP TABLE {temp_table_name} ({','.join(cols)})"

       with conn.cursor() as cursor:
           cursor.execute(ddl)

           with ThreadPoolExecutor(max_workers=max_workers) as executor:
               for batch in table.to_batches():
                   executor.submit(load_batch_to_temp_table, batch, encoder, cursor)

           # Optionally load into the actual table
           # cursor.execute(f"INSERT INTO {table_name} SELECT * FROM {temp_table_name}")
           conn.commit()
   except psycopg2.Error as e:
       print(f"Error loading data to PostgreSQL: {e}")
       conn.rollback()
       raise
   finally:
       with conn.cursor() as cursor:
           cursor.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
           conn.commit()

if __name__ == "__main__":
   bucket_name = "tfmv"
   file_name = "flights.feather"
   table_name = "flights"
   conn = psycopg2.connect(
       host="localhost",
       port=5432,
       dbname="postgres",
       user="postgres",
       password="postgres",
   )
   load_feather_to_table(bucket_name, file_name, table_name, conn)
   conn.close()
