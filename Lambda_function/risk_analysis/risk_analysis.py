import boto3
import numpy as np
import pandas as pd
import awswrangler as wr
from botocore.client import ClientError
import quantstats as qs
qs.extend_pandas()
from datetime import datetime
s3_bucket = "nasdaq-stocks"
glue_catalog_db = "demo-nasdaq-catalog-db"
glue_catalog_tb = "nasdaq_stocks"
dynamo_tb = 'PortfolioSubscribers'
my_session = boto3.Session(region_name="us-east-1")
lambda_client = boto3.client('lambda',region_name="us-east-1")
dynamo_client = boto3.client('dynamodb',region_name="us-east-1")
ses_client = boto3.client('ses',region_name="us-east-1")

subscribers = dynamo_client.scan(
    TableName=dynamo_tb
)

stocks = ''
for i in subscribers['Items']:
    stocks += ','+ i['symbol']['S']
stocks = stocks[1:]
stocks = stocks.split(sep=',')
symbol_list = list(set(stocks))
symbol_list = (','.join(symbol_list))

# Query time series of all symbols we need:
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

def sqs_message(i,report_html):
    date = datetime.now().strftime('%Y-%m-%d')
    # This address must be verified with Amazon SES.
    SENDER = "vomanhtien12345@gmail.com"

    # Replace recipient@example.com with a "To" address. If your account 
    # is still in the sandbox, this address must be verified.
    RECIPIENT = i['mail_address']['S']

    # The subject line for the email.
    SUBJECT = f"Risk report of Portfolio({i['symbol']['S'].upper()}) ({date})"
             
    # The HTML body of the email.
    BODY_HTML = f"""
                {report_html}
                """            

    # The character encoding for the email.
    CHARSET = "UTF-8"

    # Try to send the email.
    try:
        #Provide the contents of the email.
        response = ses_client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': BODY_HTML,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
        )
    # Display an error if something goes wrong.	
    except ClientError as e:
        print(e)
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])

def risk_analysis(i,df): 
    # Calculate stock returns
    symbol = i['symbol']['S'].split(sep=',')
    portfolio_weights = np.array([float(x['S']) for x in i['portfolio_weight']['L']])

    # Calculate stock returns
    stock_returns = df[symbol].pct_change(periods=1)
    stock_returns.drop(stock_returns.index[0], axis=0, inplace=True)

    # Calculate the weighted stock returns
    weighted_returns = stock_returns.mul(portfolio_weights, axis=1)

    # Calculate the portfolio returns
    portfolio_returns = weighted_returns.sum(axis=1)
    report_df = qs.reports.metrics(mode='basic',returns= portfolio_returns, display = False)
    report_html = report_df.to_html()

    sqs_message(i,report_html)

def handler(event,context):
    for i in subscribers['Items']:
        risk_analysis(i,df)

