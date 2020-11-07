from datetime import date
from pyspark.sql import SparkSession
from pyspark.ml.feature import StopWordsRemover, RegexTokenizer, Word2VecModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.classification import MultilayerPerceptronClassificationModel
from pyspark.sql.functions import col,regexp_replace, udf, array_max
from pyspark.sql.types import *

if __name__ == '__main__':
    
    bucket_name = "web-app-project"
    spark = SparkSession\
        .builder\
        .appName("sentiment_analysis")\
        .getOrCreate()

    today = date.today().isoformat()
    tweets_path = "s3://" + bucket_name + "/data/twitter-data-" + today + ".json" 
    tweets_df = spark.read.json(tweets_path)
    
    # only keep tweets in english
    tweets_df = tweets_df.filter(tweets_df.lang == "en")
    tweets_df = tweets_df.withColumn("cleaned_tweets", regexp_replace(col("tweets"), "http.+|@.|\n|RT|\d+", ' '))
    # All words are lowercase and tokenized
    tweets_df = RegexTokenizer(inputCol="cleaned_tweets", outputCol="lowercase_tweets", pattern="\\W").transform(tweets_df)
    # We remove the StopWords
    tweets_df = StopWordsRemover(inputCol="lowercase_tweets", outputCol="processed_tweets").transform(tweets_df)
    # We drop the unused columns
    tweets_df = tweets_df.drop("cleaned_tweets", "lowercase_tweets", "lang", "date")
    # We load the language model
    model_path = "s3://" + bucket_name + "/models/w2v_model"
    loaded_model = Word2VecModel.load(model_path)
    # We add the output columns : it is the average of the words' vectors for each tweet
    tweets_df = loaded_model.transform(tweets_df)

    # We load the classifier
    clf_path = "s3://" + bucket_name + "/models/mpc_model"
    loaded_clf = MultilayerPerceptronClassificationModel.load(clf_path)
    predictions = loaded_clf.transform(tweets_df)

    # We keep the probability only for the predicted sentiment
    to_array = udf(lambda v: v.toArray().tolist(), ArrayType(FloatType()))
    predictions = predictions.withColumn("probability", to_array("probability"))
    predictions = predictions.withColumn("probability", array_max("probability"))

    # We assign a weight of 0.5 to negative tweets 
    compute_weights = udf(lambda x: x if x == 1.0 else 0.5, FloatType())

    # The sentiment score is in [0, 0.5] if the value is negative and [0.5, 1] if positive
    predictions = predictions.withColumn("weights", compute_weights("prediction")) \
    .withColumn("sentiment_score", col("probability")*col("weights")) \
    .groupBy("symbol") \
    .agg({"sentiment_score" : "avg"})\
    .withColumnRenamed("avg(sentiment_score)", "sentiment_score") \
    .drop("features", "rawPrediction", "processed_tweets")

    predictions.repartition(1).write.mode("overwrite").csv("hdfs:///sentiment", header=True)
