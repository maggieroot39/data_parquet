use polars::prelude::*;
use std::fs;

fn main() -> PolarsResult<()> {
    let dir_path = "stock_market_data/nyse/csv/";


    //let mut file = std::fs::File::open("stock_market_data/nyse/csv/*.parquet").unwrap();

   // let df2 = ParquetReader::new(&mut file).finish().unwrap();


//    let df = LazyFrame::scan_parquet("data/*.parquet", ScanArgsParquet::default())?
    let df = LazyFrame::scan_parquet("stock_market_data/nasdaq/csv/*.parquet", ScanArgsParquet::default())?
        .select([
            // select all columns
            all(),
            // and do some aggregations
//            cols(["col_0", "col_1"]).sum().suffix("_summed"),
            cols(["Date", "Low"]).sum().suffix("_summed"),
        ])
        .collect()?;

    println!("{}", df);
    Ok(())
}
