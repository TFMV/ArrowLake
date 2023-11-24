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


import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyarrow import csv as pa_csv
from pyarrow import feather as pa_feather
from pyarrow import fs
from pyarrow import json as pa_json
from pyarrow import orc as pa_orc
from pyarrow import parquet as pa_parquet


def list_files(bucket_name):
    try:
        gcs_filesystem = fs.GcsFileSystem()
        file_selector = fs.FileSelector(f"{bucket_name}", recursive=True)
        files_info = gcs_filesystem.get_file_info(file_selector)
        return [
            file_info.path
            for file_info in files_info
            if file_info.type == fs.FileType.File
        ]
    except Exception as e:
        print(f"Error listing files in bucket {bucket_name}: {e}")
        return []


def convert_file(file_path, bucket_name):
    try:
        gcs_filesystem = fs.GcsFileSystem()
        file_name = os.path.basename(file_path)
        _, ext = os.path.splitext(file_name)

        with gcs_filesystem.open_input_stream(file_path) as f:
            if f.read(1) == b"":  # Check if the file is empty
                print(f"Skipping empty file {file_path}")
                return None
            f.seek(0)  # Reset file pointer if not empty

            if ext.lower() in [".csv", ".json", ".parquet", ".orc"]:
                if ext.lower() == ".csv":
                    table = pa_csv.read_csv(f)
                elif ext.lower() == ".json":
                    table = pa_json.read_json(f)
                elif ext.lower() == ".parquet":
                    table = pa_parquet.read_table(f)
                elif ext.lower() == ".orc":
                    table = pa_orc.read_table(f)

                with tempfile.NamedTemporaryFile(
                    suffix=".feather", delete=False
                ) as tmp:
                    pa_feather.write_feather(table, tmp.name)

                    with gcs_filesystem.open_output_stream(
                        f"{bucket_name}/{file_name.rsplit('.', 1)[0]}.feather"
                    ) as out_f:
                        tmp.seek(0)
                        out_f.write(tmp.read())
                        print(
                            f"Converted {file_path} to {file_name.rsplit('.', 1)[0]}.feather"
                        )
        return file_name
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return None


def detect_and_convert_files(bucket_name, max_workers=4):
    file_paths = list_files(bucket_name)
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(convert_file, file_path, bucket_name): file_path
            for file_path in file_paths
        }
        for future in as_completed(future_to_file):
            file_path = future_to_file[future]
            result = future.result()
            if result:
                results.append(result)

    print(f"Converted files: {results}")


if __name__ == "__main__":
    bucket_name = "tfmv"
    detect_and_convert_files(bucket_name)
