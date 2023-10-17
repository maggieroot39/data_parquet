# data_with_parquet

1. download data from https://www.kaggle.com/datasets/paultimothymooney/stock-market-data/
1. unzip the data to e.g. stock_market_data
1. create venv
    `python -m venv .venv`
1. install requirements
    `pip install pandas pyarrow`
1. modify line 11 of the `process_data.py` if needed
1. `python process_data.py`
