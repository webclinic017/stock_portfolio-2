import pandas as pd
from alpaca.data import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
import awswrangler as wr
import boto3
import requests
from botocore.exceptions import ClientError


s3_client = boto3.client('s3')
my_session = boto3.Session(region_name="us-east-1")
s3_bucket = "nasdaq-stocks"
glue_catalog_db = "demo-nasdaq-catalog-db"
glue_catalog_tb = "nasdaq-stocks"
parquet_s3_path = f"s3://{s3_bucket}/stocks/"

stock_url ='https://data.alpaca.markets/v2/stocks/bars'
nasdaq_url = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=10000&exchange=nasdaq"
api_key = 'PK80ONHH7G3C8POM1OM8'
secret_key = 'aJRVmTbA2DGE3QL4uE2Xpe02gboAiWKWk28cOJFR'
stock_client = StockHistoricalDataClient(api_key, secret_key)
s3_bucket = "demo-nasdaq-s3"
headers = {
    "User-Agent" : "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36",
}

stock_client = StockHistoricalDataClient(api_key, secret_key)

# Only for initial data ingestion
symbol_response = requests.get(nasdaq_url, headers=headers).json()['data']['table']['rows']
symbol_table = pd.DataFrame.from_dict(data = symbol_response)
nA_marketcap = symbol_table[symbol_table['marketCap']=='NA'].index
symbol_table = symbol_table.drop(nA_marketcap, axis=0)
symbol_table['marketCap'].replace(',','',regex=True, inplace=True)
symbol_table['marketCap'] = symbol_table['marketCap'].astype('int')
symbol_table = symbol_table[symbol_table['marketCap']>=1000000000] # Delete symbol with marketcap < 1 billion 
filter_list = symbol_table['symbol'].to_list() # Create fixed symbols list for fixed schema

print (f'Number of symbols from nasdaq web: {len(filter_list)}')

# Get time series data of all stocks
request_params = StockBarsRequest(
                            symbol_or_symbols = filter_list,
                            timeframe = TimeFrame.Day,
                            start = "1900-01-01 00:00:00",
                            end = "2022-10-29 00:00:00"
)

bars = stock_client.get_stock_bars(request_params)
raw_df = bars.df
raw_df.reset_index(inplace=True)
raw_df.iloc[:,2:] = raw_df.iloc[:,2:].astype('float')
raw_df['timestamp'] = raw_df['timestamp'].dt.tz_localize(None)
raw_df['timestamp']  = raw_df['timestamp'].dt.strftime('%Y-%m-%d')
print ('Successfully retrieved historical data')
print (f"Number of symbols from alpaca api: {raw_df['symbol'].nunique()}")

index_start = raw_df['timestamp'].min()
index_end = raw_df['timestamp'].max()
time_interval = pd.date_range(start=index_start, end=index_end, freq='D')
time_interval = time_interval.format(formatter=lambda x: x.strftime('%Y-%m-%d'))
df = pd.DataFrame(data = time_interval, columns=['timestamp'])

for i,j in raw_df.groupby('symbol'):
    j = j.rename(columns={'close':f'{i}'})
    df = df.merge(j[['timestamp',i]], on='timestamp', how='left')
rows = len(df.axes[0])
cols = len(df.axes[1])
print ('Successfully transform the historical data ')
print("Number of Rows: ", rows)
print("Number of Columns: ", cols)

# Write parquet file to s3 and link to glue data catalog
columns_types, partitions_types = wr.catalog.extract_athena_types(
    df=df,
    file_format="parquet",
    index=False,
)

try:
    wr.catalog.create_parquet_table(
        database=glue_catalog_db,
        table=glue_catalog_tb,
        path=parquet_s3_path,
        columns_types=columns_types,
        compression='snappy',
        boto3_session = my_session
        )
except ClientError as e:
    print (e)

try:
    stock_res = wr.s3.to_parquet(
        df=df,
        path=parquet_s3_path,
        dataset=True,
        database=glue_catalog_db,
        table=glue_catalog_tb,
        mode="append",
        boto3_session = my_session
    )
except ClientError as e:
    print (e)

