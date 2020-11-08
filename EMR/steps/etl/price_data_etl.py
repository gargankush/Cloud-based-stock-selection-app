import boto3
import requests
import yaml
import json
import time
from datetime import date, timedelta, datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *

def alphavantage_api_call(api_key, symbol, interval="1min"):
    args = f"function=TIME_SERIES_INTRADAY&symbol={symbol}&interval={interval}&apikey={api_key}&outputsize=full"
    url = f"https://www.alphavantage.co/query?{args}"
    response = requests.get(url)
    return response

if __name__ == "__main__":

    bucket_name = "web-app-project"
    spark = SparkSession\
        .builder\
        .appName("etl")\
        .getOrCreate()
   
    s3 = boto3.client('s3')
    yaml_file = s3.get_object(Bucket=bucket_name, Key="config.yaml")
    yaml_file = yaml.safe_load(yaml_file["Body"].read().decode("utf-8"))
    api_key = yaml_file["alphavantage"]["api_key"]
    body = s3.get_object(Bucket=bucket_name, Key="data/symbols.txt")['Body'].read()
    symbols = body.decode("utf8").split('\n')
    
    schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("timeframe", StringType(), True), 
    StructField("open", StringType(), True),
    StructField("high", StringType(), True),
    StructField("low", StringType(), True),
    StructField("close", StringType(), True),
    StructField("volume", StringType(), True)
    ])
    
    row = []
    today = date.today().isoformat()

    for symbol in symbols:
        try:
            price_data = alphavantage_api_call(api_key, symbol)
            time.sleep(13)
            price_data = json.loads(price_data.text)["Time Series (1min)"]
            for tf, price in price_data.items():
                row.append((symbol, 
                           tf, 
                           price["1. open"], 
                           price["2. high"], 
                           price["3. low"],
                           price["4. close"],
                           price["5. volume"]))
        except:
            continue
    # We run the workflow from tuesday to saturday after the close of the stock exchange
    # If we are tuesday, the last data is from saturday
    if datetime.today().weekday() == 1:
        previous = (datetime.today() - timedelta(days=2)).isoformat()
    else: 
        previous = (date.today() - timedelta(days=1)).isoformat()
    old_df = spark.read.csv('s3://' + bucket_name + '/data/price-data-' + previous + '.csv', header=True, inferSchema=True)
    new_df = spark.createDataFrame(row, schema)
    df = new_df.union(old_df)
    df.repartition(1).write.csv('s3://' + bucket_name + '/data/price-data-' + today, header=True)
    # rename file and delete previous data
    response = s3.list_objects(Bucket=bucket_name, Prefix="data/price-data-" + today)
    files = [response["Contents"][i]["Key"] for i in range(len(response["Contents"]))]
    files.append("data/price-data-" + previous + ".csv")
    for f in files:
        if "part" in f:
            s3.copy_object(
                    ACL='public-read',
                    Bucket=bucket_name,
                    CopySource=bucket_name + "/" + f,
                    Key="data/price-data-" + today + ".csv")
            s3.delete_object(Bucket=bucket_name, Key=f)
        else:
            s3.delete_object(Bucket=bucket_name, Key=f)
