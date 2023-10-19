use polars::prelude::*;
use std::fs;

fn main() -> PolarsResult<()> {
    let dir_path = "./../../500_file_folder";

    // Collect paths of all Parquet files in the directory
    let paths: Vec<_> = fs::read_dir(&dir_path)?
    .filter_map(Result::ok)
    .filter(|entry| entry.path().extension().and_then(std::ffi::OsStr::to_str) == Some("parquet"))
    .collect();

    println!("{}", paths[0].path().display());


   // Loop through each Parquet file and concatenate to the main DataFrame
   for path in paths {
        println!("{}", path.path().display());
        let mut file = std::fs::File::open(path.path()).unwrap();
        let df3 = ParquetReader::new(&mut file).finish().unwrap();

        println!("{}", df3);
  }

    //let mut file = std::fs::File::open("stock_market_data/nyse/csv/*.parquet").unwrap();

   // let df2 = ParquetReader::new(&mut file).finish().unwrap();


//    let df = LazyFrame::scan_parquet("data/*.parquet", ScanArgsParquet::default())?




    let df = LazyFrame::scan_parquet("/Users/cliff/rust/data2/data_parquet/10_file_folder/*.parquet", ScanArgsParquet::default())?
        .select([
            // select all columns
            all(),
            // and do some aggregations
//            cols(["col_0", "col_1"]).sum().suffix("_summed"),
            cols(["High", "Low"]).sum().suffix("_summed"),
        ])
        .collect()?;

    println!("{}", df);
    Ok(())
}
