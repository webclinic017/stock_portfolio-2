import pandas as pd
import numpy as np
pd.options.mode.chained_assignment = None
import awswrangler as wr
import boto3
my_session = boto3.Session(region_name="us-east-1")

from pypfopt.expected_returns import ema_historical_return
from pypfopt import EfficientFrontier, EfficientCVaR
from pypfopt.discrete_allocation import DiscreteAllocation, get_latest_prices

s3_bucket = "nasdaq-stocks"
glue_catalog_db = "demo-nasdaq-catalog-db"
glue_catalog_tb = "nasdaq_stocks"

def handler(event,context):
    symbol_list = event['symbol'].lower()
    my_session = boto3.Session(region_name="us-east-1")

    # Query time series data from datalake
    df = wr.athena.read_sql_query(
        sql=f"select timestamp,{symbol_list} from {glue_catalog_tb}", 
        ctas_approach=False,
        database=glue_catalog_db,
        data_source='AwsDataCatalog',
        boto3_session=my_session,
        keep_files=False
    )
    df.set_index('timestamp', inplace=True)
    df.sort_values(by='timestamp',inplace=True)
    df.drop_duplicates(inplace=True)
    df.index = pd.to_datetime(df.index)
    df.dropna(axis=0, how='all', inplace=True)
    stock_returns = df.pct_change(periods=1)
    stock_returns.drop(stock_returns.index[0], axis=0, inplace=True)
    
    # Compute the annualized average historical return
    expected_returns = ema_historical_return(stock_returns, returns_data=True, frequency=252, span=200)

    # Compute the sample covariance matrix of returns
    sample_cov = stock_returns.cov() * 252

    # Optimize the portfolio
    method = event['method']
    if hasattr(EfficientFrontier, method):
        ef = EfficientFrontier(expected_returns, sample_cov)   
        if method == 'efficient_risk':
            weight = eval(f"ef.{method}(target_volatility  = {event['additional_param']})")  
        elif method == 'efficient_return':
            weight = eval(f"ef.{method}(target_return = {event['additional_param']})")  
        else:
            weight = eval(f"ef.{method}()")
    else:
        ef = EfficientCVaR(expected_returns, stock_returns, beta=0.95)
        weight = eval(f'ef.{method}()')

    # Portfolio allocation  
    clean_weights = ef.clean_weights()
    latest_price = get_latest_prices(df)
    discrete_allocation = DiscreteAllocation(
        weights=clean_weights, 
        latest_prices=latest_price,
        total_portfolio_value=event["total_portfolio_value"]
    )
    portfolio_performance = ef.portfolio_performance(verbose=True)
    number_shares, leftover = discrete_allocation.greedy_portfolio()
    time_interval = f"From {stock_returns.index[0]} to {stock_returns.index[-1]}"
    return (clean_weights, portfolio_performance, number_shares, leftover, time_interval)
