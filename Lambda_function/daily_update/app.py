import pandas as pd
import numpy as np
import awswrangler as wr
import boto3
import requests
from datetime import datetime, timedelta

my_session = boto3.Session(region_name="us-east-1")
s3_bucket = "demo-nasdaq-s3"
parquet_s3_path = f"s3://{s3_bucket}/stocks/"
glue_catalog_db = "demo-nasdaq-catalog-db"
glue_catalog_tb = "nasdaq-stocks"

stock_url ='https://data.alpaca.markets/v2/stocks/bars'
headers = {
    'APCA-API-KEY-ID':'PK80ONHH7G3C8POM1OM8',
    'APCA-API-SECRET-KEY':'aJRVmTbA2DGE3QL4uE2Xpe02gboAiWKWk28cOJFR'
}

def handler(event,context):
    # Create a string include all symbol for get request
    pars = wr.catalog.get_columns_comments(database=glue_catalog_db, table=glue_catalog_tb, boto3_session = my_session)
    a = list(pars.keys())
    a.remove('timestamp')
    glue_symbols= [i.upper() for i in a]
    symbol_list = (', '.join(glue_symbols))
    symbol_list = symbol_list.replace(' ','')
    print (f'Number of symbols in schema: {len(glue_symbols)}')

    # params for get request
    update_date = (datetime.now()).strftime('%Y-%m-%d')
    data= dict()
    data['symbols'] = symbol_list
    data['timeframe'] = '1Day'
    data['start'] = update_date
    data['limit'] = 7000

    # Send get request to get daily price of stocks
    try:
        daily_price = requests.get(url=stock_url,headers=headers, params=data).json()['bars']
        print ('Successfully retrieved stock tickers')
    except Exception as e:
        print (e)

    if len(daily_price) == 0:
        return {
            'statusCode': 200,
            'body': f"No price update in {update_date}."
        }

    df = pd.DataFrame(data={i:j[0] for i,j in daily_price.items()})
    df.drop(['t','o','h','l','v','n','vw'], inplace=True)
    
    df.reset_index(inplace=True, drop = True)
    df['timestamp'] = update_date

    # Check if all symbols in the list have daily price, else fill nan
    available_symbols = list(daily_price.keys())
    unavailable_symbol = np.setdiff1d(glue_symbols,available_symbols)
    print (f"Number of stocks available from alpaca api: {len(available_symbols)}, and {len(unavailable_symbol)} symbols must be filled with nan ")
    for i in unavailable_symbol:
        df[i] = np.nan

    # Write to Datalake 
    try:
        daily_update = wr.s3.to_parquet(
            df=df,
            path=parquet_s3_path,
            dataset=True,
            database=glue_catalog_db,
            table=glue_catalog_tb,
            mode="append",
            schema_evolution = False,
            boto3_session = my_session
        )
        print ('Successfully uploaded daily price data')
    except Exception as e:
        print (e)     

    return {
    'statusCode': 200,
    'body': f"Successfully update daily price for {update_date}."
    }