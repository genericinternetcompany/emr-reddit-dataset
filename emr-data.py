
import sys

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext, SQLContext




if __name__ == "__main__":

    print(len(sys.argv))
    if (len(sys.argv) != 3):
        print("Usage: spark-etl [input-folder] [output-folder]")
        sys.exit(0)

    spark = SparkSession\
        .builder\
        .appName("emr-reddit-data")\
        .getOrCreate()

    sqlContext = SQLContext(spark)

    fields = [StructField("archived", BooleanType(), True), 
          StructField("author", StringType(), True), 
          StructField("author_flair_css_class", StringType(), True), 
          StructField("body", StringType(), True),  
          StructField("controversiality", LongType(), True), 
          StructField("created_utc", StringType(), True), 
          StructField("distinguished", StringType(), True), 
          StructField("downs", LongType(), True), 
          StructField("edited", StringType(), True), 
          StructField("gilded", LongType(), True), 
          StructField("id", StringType(), True), 
          StructField("link_id", StringType(), True), 
          StructField("name", StringType(), True), 
          StructField("parent_id", StringType(), True), 
          StructField("retrieved_on", LongType(), True), 
          StructField("score", LongType(), True), 
          StructField("score_hidden", BooleanType(), True), 
          StructField("subreddit", StringType(), True), 
          StructField("subreddit_id", StringType(), True), 
          StructField("ups", LongType(), True)] 

    rawDF = sqlContext.read.json("s3://reddit-comments/2015/RC_2015-05", StructType(fields)).registerTempTable("comments")

    distinct_gilded_authors_by_subreddit = sqlContext.sql(""" 
    SELECT subreddit, COUNT(DISTINCT author) as authors 
    FROM comments 
    WHERE gilded > 0 
    GROUP BY subreddit 
    ORDER BY authors DESC 
    """)

    average_score_by_subreddit = sqlContext.sql(""" 
    SELECT subreddit, AVG(score) as avg_score 
    FROM comments 
    GROUP BY subreddit 
    ORDER BY avg_score DESC 
    """)

    print(distinct_gilded_authors_by_subreddit)

    distinct_gilded_authors_by_subreddit.write.parquet(sys.argv[1])

    average_score_by_subreddit.write.parquet(sys.argv[2])