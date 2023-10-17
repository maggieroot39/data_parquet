extern crate polars;

use std::fs::File;
use std::path::Path;
use parquet::file::reader::{FileReader, SerializedFileReader};

fn main() {
    let file = File::open(&Path::new("/Users/damask/code/arrow_rust_example/stock_market_data/forbes2000/csv/A.parquet")).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let mut iter = reader.get_row_iter(None).unwrap();
    
    // print the actual records
    while let Some(record) = iter.next() {
        println!("{:?}", record);
    }

    let file = File::open(&Path::new("/Users/damask/code/arrow_rust_example/stock_market_data/forbes2000/csv/A.parquet")).expect("Couldn't open parquet data");
    let reader = SerializedFileReader::new(file).unwrap();
    let parquet_metadata = reader.metadata();
    let _rows = parquet_metadata.file_metadata().num_rows();

    let fields = parquet_metadata.file_metadata().schema().get_fields();

    // just print the column names
    for (pos, column) in fields.iter().enumerate() {
        let name = column.name();
        println!("Column {}: {}", pos, name);
    }
}

