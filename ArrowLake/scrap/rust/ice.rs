use iceberg::Table;
use pyarrow::feather;
use pandas::DataFrame;

fn iceberg_to_feather(iceberg_table_path: &str, output_file_path: &str) {
   let table = Table::new_table(iceberg_table_path);
   let df = table.to_pandas();
   feather::write_feather(df, output_file_path);
}
