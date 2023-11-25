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

import pandas as pd
import pyarrow.fs as pafs

def read_feather_files_from_gcs(bucket_name):
    gcs_filesystem = pafs.GcsFileSystem()
    file_selector = pafs.FileSelector(f"gs://{bucket_name}", recursive=True)
    files_info = gcs_filesystem.get_file_info(file_selector)

    dataframes = []
    for file_info in files_info:
        if file_info.path.endswith('.feather'):
            with gcs_filesystem.open_input_file(file_info.path) as f:
                df = pd.read_feather(f)
                dataframes.append(df)

    return pd.concat(dataframes, ignore_index=True)

bucket_name = 'tfmv'
all_data = read_feather_files_from_gcs(bucket_name)
print(all_data)
