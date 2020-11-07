from pyspark.sql import SparkSession, Window
from datetime import date, timedelta, datetime
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql.functions import col, udf, collect_list, dayofyear, to_date
from pyspark.sql.functions import min as Min
from pyspark.sql.functions import max as Max
from pyspark.sql.types import *

if __name__ == '__main__':

    bucket_name = "web-app-project"
    spark = SparkSession\
        .builder\
        .appName("price_prediction")\
        .getOrCreate()

    today = date.today().isoformat()
    prices_path = "s3://" + bucket_name + "/data/price-data-" + today + ".csv" 
    df = spark.read.csv(prices_path, header=True)
    
    # extract day of year from timestamp and convert column types
    df = df.withColumn("timeframe", col("timeframe").cast(TimestampType()))
    df = df.withColumn("dayofyear", dayofyear("timeframe"))
    for col_name in df.columns[2:]:
        df = df.withColumn(col_name, col(col_name).cast(FloatType()))

    # filter data from last two weeks
    # two_weeks_ago = (date.today() - timedelta(days=13))
    # df = df.filter(to_date(df["timeframe"]) > two_weeks_ago)
    
    # extract open, high, low, close of the last known price
    window = Window.partitionBy()
    price_data = df.withColumn("last_day", Max(col("dayofyear")).over(window)) \
    .filter(col("dayofyear") == col("last_day")) \
    .drop("max", "dayofyear") \
    .withColumn("high", Max(col("high")).over(Window.partitionBy("symbol").orderBy())) \
    .withColumn("low", Min(col("low")).over(Window.partitionBy("symbol").orderBy())) 

    temp = price_data.select("symbol","timeframe", "open") \
    .withColumn("open_time", Min(col("timeframe")).over(Window.partitionBy("symbol").orderBy())) \
    .filter(col("timeframe") == col("open_time")) \
    .drop("timeframe", "open_time")

    ohlc = temp.join(price_data.select("symbol", "high", "low", "close", "last_day", "timeframe") \
            .withColumn("close_time", Max(col("timeframe")).over(Window.partitionBy("symbol").orderBy())) \
            .filter(col("timeframe") == col("close_time")) \
            .drop("timeframe", "open_time", "last_day", "close_time"), ["symbol"])

    # aggregate by day
    df = df.groupBy("symbol", "dayofyear") \
    .agg({"open": "avg"}) \
    .orderBy("symbol", "dayofyear", ascending=[1, 1]) \
    .filter(df.dayofyear.isNotNull())

    # create a features column : list of open prices averaged by day
    df = df.groupby('symbol').agg(collect_list('avg(open)').alias("features"))

    # add a yearly average column
    yearly_avg = udf(lambda x: sum(x)/len(x), DoubleType())
    df = df.withColumn("yearly_average", yearly_avg("features"))

    # convert to vectors for the linear regression model
    array_to_vector = udf(lambda x: Vectors.dense(x[0]), VectorUDT())
    df = df.withColumn("features", array_to_vector("features"))

    # load the model and apply it
    model_path = "s3://" + bucket_name + "/models/lr_model"
    loaded_model = LinearRegressionModel.load(model_path)
    results = loaded_model.evaluate(df)
    predictions = results.predictions
    predictions = predictions.withColumn("performance",  ((col("prediction")/col("yearly_average")) - 1) * 100)
    performances = predictions.select("performance").rdd.map(lambda x: x[0]).collect()
    min_value = min(performances)
    max_value = max(performances)
    normalize = udf(lambda x: (x - min_value) / (max_value - min_value), FloatType())
    
    # the score is the predicted price compared to the yearly average (normalized)
    predictions = predictions.select("symbol", "prediction", "performance") \
                             .withColumn("price_score", normalize("performance")) \
                             .drop("performance")
    
    predictions.repartition(1).write.mode("overwrite").csv("hdfs:///price", header=True)
    ohlc.repartition(1).write.mode("overwrite").csv("hdfs:///ohlc", header=True)
