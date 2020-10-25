import time
import boto3
import requests
import yaml
import json
import random
import datetime
from datetime import date, timedelta, datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    
    def twitter_api_call(api_key, symbol, start_time, end_time, max_results=100):
        args = f"max_results={max_results}&query={symbol}&start_time={start_time}&end_time={end_time}"
        url = f"https://api.twitter.com/2/tweets/search/recent?{args}"
        headers = {"Authorization": "Bearer {}".format(api_key)}
        response = requests.request("GET", url, headers=headers)
        return response.json()

    def extract_tweets(twitter_json):
        tweets = []
        for i in range(len(twitter_json["data"])):
            tweets.append(twitter_json["data"][i]["text"])
        return tweets

    s3 = boto3.client("s3")
    yaml_file = s3.get_object(Bucket="cse6242-neren3", Key="config.yaml")
    yaml_file = yaml.safe_load(yaml_file["Body"].read().decode("utf-8"))
    api_key = yaml_file["twitter"]["bearer_token"]

    bucket_name = "team166project"
    spark = SparkSession\
            .builder\
            .appName("etl")\
            .getOrCreate()

    body = s3.get_object(Bucket=bucket_name, Key="symbols.txt")['Body'].read()
    symbols = body.decode("utf8").split('\n')

    schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("date", StringType(), True), 
    StructField("tweets", StringType(), True)
    ])

    row = []
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    today = date.today().isoformat()
    week_ago = (date.today() - timedelta(days=6)).isoformat()
    
    start_time = (datetime.utcnow() - timedelta(days=1)).isoformat("T") + "Z" 
    end_time = (datetime.utcnow()- timedelta(hours=1)).isoformat("T") + "Z"
    old_df = spark.read.json('s3://' + bucket_name + '/twitter-data-' + yesterday + ".json")
    random.shuffle(symbols)

    for symbol in symbols[:450]:
        try:
            twitter_json = twitter_api_call(api_key, symbol, start_time, end_time, max_results=100)
            tweets = extract_tweets(twitter_json)
            for tweet in tweets:
                  row.append((symbol, today, tweet))
        except:
            continue
        
    new_df = spark.createDataFrame(row, schema)
    df = new_df.union(old_df)
    # keep data for trailing week
    df = df.filter(df["date"] != week_ago)
    df.repartition(1).write.json('s3://' + bucket_name + '/twitter-data-' + today)
    # rename file and delete old files
    time.sleep(60)
    response = s3.list_objects(Bucket=bucket_name, Prefix="twitter-data-" + today)
    files = [response["Contents"][i]["Key"] for i in range(len(response["Contents"]))]
    files.append("twitter-data-" + yesterday + ".json")
    for f in files:
        if "part" in f:
            s3.copy_object(
            ACL='public-read',
            Bucket=bucket_name,
            CopySource=bucket_name + "/" + f,
            Key="twitter-data-" + today + ".json")
            s3.delete_object(Bucket=bucket_name, Key=f)
        else:
            s3.delete_object(Bucket=bucket_name, Key=f) 
