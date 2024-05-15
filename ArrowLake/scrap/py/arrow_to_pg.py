# --------------------------------------------------------------------------------
# Author: Thomas F McGeehan V
#
# This file is part of a software project developed by Thomas F McGeehan V.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# For more information about the MIT License, please visit:
# https://opensource.org/licenses/MIT
#
# Acknowledgment appreciated but not required.
# --------------------------------------------------------------------------------


import tempfile
import time
from concurrent.futures import ThreadPoolExecutor

import psycopg2
import pyarrow as pa
import pyarrow.feather as feather
from pgpq import ArrowToPostgresBinaryEncoder
from pyarrow.fs import GcsFileSystem


def load_batch_to_temp_table(batch, encoder, cursor):
    with cursor.copy(f"COPY temp_data FROM STDIN WITH (FORMAT BINARY)") as copy:
        copy.write(encoder.write_batch(batch))


def load_feather_to_table(
    bucket_name, file_name, table_name, conn, batch_size=10000, max_workers=4
):
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
        cols = [
            f'"{col_name}" {col.data_type.ddl()}' for col_name, col in pg_schema.columns
        ]
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
