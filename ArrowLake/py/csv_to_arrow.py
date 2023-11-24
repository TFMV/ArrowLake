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

import pyarrow.csv as pc
import pyarrow.feather as feather
from pyarrow.fs import GcsFileSystem

def csv_gz_to_arrow_gcs(csv_gz, bucket_name, file_name):
    try:
        table = pc.read_csv(csv_gz, read_options=pc.ReadOptions(autogenerate_column_names=True), parse_options=pc.ParseOptions(delimiter=','))
        print(f"Read {table.num_rows} rows")

        with GcsFileSystem().open_output_stream(f"{bucket_name}/{file_name}") as f:
            feather.write_feather(table, f)
            return table.num_rows
    except Exception as e:
        print(e)
        return "Error"

if __name__ == "__main__":
    print("Running locally")
    bucket_name = "tfmv"
    file_name = "flights.feather"
    csv_gz = "/Users/thomasmcgeehan/VDS/veloce/arrowlake/data/flights.csv.gz"
    row_count = csv_gz_to_arrow_gcs(csv_gz, bucket_name, file_name)
    print(f"Rows written: {row_count}")
