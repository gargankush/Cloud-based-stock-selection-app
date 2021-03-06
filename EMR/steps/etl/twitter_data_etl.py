import boto3
import requests
import yaml
import json
import datetime
from datetime import date, timedelta, datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *

def twitter_api_call(api_key, symbol, start_time, end_time, max_results=100):
    args = f"max_results={max_results}&tweet.fields=lang&query={symbol}&start_time={start_time}&end_time={end_time}"
    url = f"https://api.twitter.com/2/tweets/search/recent?{args}"
    headers = {"Authorization": "Bearer {}".format(api_key)}
    response = requests.request("GET", url, headers=headers)
    return response.json()

def extract_tweets(twitter_json):
    tweets = []
    lang = []
    for i in range(len(twitter_json["data"])):
        tweets.append(twitter_json["data"][i]["text"])
        lang.append(twitter_json["data"][i]["lang"])
    return tweets, lang

if __name__ == "__main__":

    bucket_name = "web-app-project"
    s3 = boto3.client("s3")
    yaml_file = s3.get_object(Bucket=bucket_name, Key="config.yaml")
    yaml_file = yaml.safe_load(yaml_file["Body"].read().decode("utf-8"))
    api_key = yaml_file["twitter"]["bearer_token"]

    spark = SparkSession\
            .builder\
            .appName("etl")\
            .getOrCreate()

    body = s3.get_object(Bucket=bucket_name, Key="data/symbols.txt")['Body'].read()
    symbols = body.decode("utf8").split('\n')
    schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("date", StringType(), True), 
    StructField("tweets", StringType(), True),
    StructField("lang", StringType(), True)
    ])
    row = []
    today = date.today().isoformat()
    # If we are tuesday, the last data is from saturday
    if datetime.today().weekday() == 1:
        previous = (datetime.today() - timedelta(days=2)).isoformat()
    else: 
        previous = (date.today() - timedelta(days=1)).isoformat()
    
    start_time = (datetime.utcnow() - timedelta(days=1)).isoformat("T") + "Z" 
    end_time = (datetime.utcnow()- timedelta(hours=1)).isoformat("T") + "Z"

    for symbol in symbols:
        try:
            twitter_json = twitter_api_call(api_key, symbol, start_time, end_time, max_results=100)
            tweets, lang = extract_tweets(twitter_json)
            for i in range(len(tweets)):
                  row.append((symbol, today, tweets[i], lang[i]))
        except:
            continue

    old_df = spark.read.json('s3://' + bucket_name + '/data/twitter-data-' + previous + ".json")
    new_df = spark.createDataFrame(row, schema)
    df = new_df.union(old_df)
    # keep data for trailing week
    week_ago = (date.today() - timedelta(days=6)).isoformat()
    # df = df.filter(df["date"] != week_ago)
    df.repartition(1).write.json('s3://' + bucket_name + '/data/twitter-data-' + today)
    # rename file and delete old files
    response = s3.list_objects(Bucket=bucket_name, Prefix="data/twitter-data-" + today)
    files = [response["Contents"][i]["Key"] for i in range(len(response["Contents"]))]
    files.append("data/twitter-data-" + previous + ".json")
    for f in files:
        if "part" in f:
            s3.copy_object(
            ACL='public-read',
            Bucket=bucket_name,
            CopySource=bucket_name + "/" + f,
            Key="data/twitter-data-" + today + ".json")
            s3.delete_object(Bucket=bucket_name, Key=f)
        else:
            s3.delete_object(Bucket=bucket_name, Key=f)
