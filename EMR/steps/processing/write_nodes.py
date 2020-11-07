import numpy as np
from pyspark.sql import SparkSession, Window
from datetime import date, timedelta, datetime
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.sql.functions import col, udf, collect_list, dayofyear, lag, round, to_date
from pyspark.sql.types import *


if __name__ == '__main__':

    bucket_name = "web-app-project"
    spark = SparkSession\
        .builder\
        .appName("write_nodes")\
        .getOrCreate()

    today = date.today().isoformat()
    prices_path = "s3://" + bucket_name + "/data/price-data-" + today + ".csv" 
    df = spark.read.csv(prices_path, header=True)

    # filter data from last two weeks
    # two_weeks_ago = (date.today() - timedelta(days=13))
    # df = df.filter(to_date(df["timeframe"]) > two_weeks_ago)

    # extract day of year from timestamp and aggregate by day
    df = df.withColumn("timeframe", col("timeframe").cast(TimestampType())) \
    .withColumn("dayofyear", dayofyear("timeframe")) \
    .groupBy("symbol", "dayofyear") \
    .agg({"open": "avg"}) \
    .orderBy("symbol", "dayofyear", ascending=[1, 1])

    # We compute the return percent for the open price between each consecutive day 
    window = Window.partitionBy("symbol").orderBy("dayofyear")
    df = df.filter(df.dayofyear.isNotNull()) \
    .withColumn("prev", lag(col("avg(open)"), 1).over(window)) 
    df = df.withColumn("percent_change", round((col("avg(open)") - col("prev")) / col("prev") * 100, 2)) \
    .filter(df.prev.isNotNull())

    # We pivot the dataframe to compute a correlation matrix
    df = df.groupBy("dayofyear") \
    .pivot("symbol") \
    .agg({"percent_change": "sum"}) \
    .orderBy("dayofyear") \
    .drop("dayofyear")
    df = df.fillna(0)
    symbols = df.columns

    # We compute the correlation matrix
    assembler = VectorAssembler(inputCols=df.columns, outputCol="features")
    df = assembler.transform(df).select("features")
    matrix = Correlation.corr(df, "features")
    n = len(symbols)
    values = np.array(matrix.collect()[0][0].values).reshape(n,n)
    schema = StructType([
        StructField("source", StringType(), True),
        StructField("target", StringType(), True), 
        StructField("value", FloatType(), True)
        ])

    # The nodes values are in a symmetric matrix 
    # Thus, we only keep the upper right triangle of the matrix
    rows = []
    for i in range(n):
        for j in range(i+1, n):
            rows.append((symbols[i], symbols[j], float(values[i][j])))
            
    def ordinal_encoder(val):
        if -0.5 <= val <= 0.5: return 0
        if -0.7 <= val < -0.5: return -1
        if val < -0.7: return -2
        if 0.5 < val <= 0.7: return 1
        if val > 0.7: return 2
    
    nodes_df = spark.createDataFrame(rows, schema)
    ordinal_encoder = udf(ordinal_encoder, StringType())
    nodes_df = nodes_df.withColumn("value", ordinal_encoder("value"))
    # we remove the low correlated nodes
    nodes_df = nodes_df.filter(nodes_df.value != 0)

    price = spark.read.csv("hdfs:///price", header=True)
    sentiment = spark.read.csv("hdfs:///sentiment", header=True)
    ohlc = spark.read.csv("hdfs:///ohlc", header=True)
    ticker_data = ohlc.join(price, ["symbol"]).join(sentiment, ["symbol"])
    # write files
    ticker_data.repartition(1).write.mode("overwrite").csv("s3://" + bucket_name + "/data/ticker-data", header=True)
    nodes_df.repartition(1).write.mode("overwrite").csv("s3://" + bucket_name + "/data/nodes", header=True)
