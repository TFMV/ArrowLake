python -c "import pandas as pd; df = pd.DataFrame({'numbers': range(1000)}); df.to_parquet('ints.parquet')" && gsutil cp ints.parquet gs://tfmv/ints.parquet
