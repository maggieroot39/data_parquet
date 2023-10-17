import os
import glob
import pandas as pd

def convert_file(filepath: str):
    parquet_name = os.path.splitext(filepath)[0]+'.parquet'
    df = pd.read_csv(filepath)
    df.to_parquet(parquet_name)

error_files=[]
for file in glob.glob("stock_market_data/*/csv/*.csv", recursive = True):
    try:
        convert_file(file)
    except:
        error_files.append(file)

print("error")
print(error_files)