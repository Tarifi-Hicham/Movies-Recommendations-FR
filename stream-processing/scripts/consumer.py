import time
import requests
import os
from dotenv import load_dotenv
import findspark
findspark.init()

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType, DateType, BooleanType
import pyspark.sql.functions as Fun
from pyspark.conf import SparkConf
import bcrypt
SparkSession.builder.config(conf=SparkConf())

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

#############################################
#         Creation of Spark Dataframe       #
#############################################

# Read the stream comming from kafka
df1 = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "moviedb") \
  .load()

df2 = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "reviewdb") \
  .load()

# Cast data as key value pair
movie_df = df1.selectExpr("CAST(value AS STRING)")

# Cast data as key value pair
reviews_df = df2.selectExpr("CAST(value AS STRING)")

#############################################
#         Define schema for data            #
#############################################

# Define the schema for movie data
movie_schema = StructType([
    StructField("adult", BooleanType(), nullable=False),
    StructField("backdrop_path", StringType(), nullable=False),
    StructField("id", IntegerType(), nullable=False),
    StructField("title", StringType(), nullable=False),
    StructField("original_language", StringType(), nullable=False),
    StructField("original_title", StringType(), nullable=False),
    StructField("overview", StringType(), nullable=False),
    StructField("poster_path", StringType(), nullable=False),
    StructField("media_type", StringType(), nullable=False),
    StructField("genre_ids", ArrayType(IntegerType()), nullable=False),
    StructField("popularity", DoubleType(), nullable=False),
    StructField("release_date", StringType(), nullable=False),
    StructField("video", BooleanType(), nullable=False),
    StructField("vote_average", DoubleType(), nullable=False),
    StructField("vote_count", IntegerType(), nullable=False)
])

# Define schema for reviews
review_schema = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("author", StringType(), nullable=False),
    StructField("author_details", StructType([
        StructField("name", StringType(), nullable=False),
        StructField("username", StringType(), nullable=False),
        StructField("avatar_path", StringType(), nullable=False),
        StructField("rating", IntegerType(), nullable=False)
    ]), nullable=False),
    StructField("content", StringType(), nullable=False),
    StructField("iso_639_1", StringType(), nullable=False),
    StructField("media_id", IntegerType(), nullable=False),
    StructField("created_at", StringType(), nullable=False),
    StructField("updated_at", StringType(), nullable=False),
    StructField("url", StringType(), nullable=False)
])

# Apply schema on data
movie_df = movie_df.withColumn("jsonData_movie", Fun.from_json(movie_df["value"], movie_schema))
reviews_df = reviews_df.withColumn("jsonData_review", Fun.from_json(reviews_df["value"], review_schema))
# reviews_df = reviews_df.withColumn("jsonData", Fun.from_json(reviews_df["value"], review_schema))

# Select attributes for the movie dataframe
movie_df = movie_df.selectExpr(
    "jsonData_movie.id as movieId",
    "jsonData_movie.title as title",
    "jsonData_movie.original_language as original_language",
    "jsonData_movie.overview as overview",
    "jsonData_movie.backdrop_path as backdrop_path",
    "jsonData_movie.poster_path as poster_path",
    "jsonData_movie.media_type as media_type",
    "explode(jsonData_movie.genre_ids) as genre_id",
    "jsonData_movie.popularity as movie_popularity",
    "jsonData_movie.release_date as release_date",
    "jsonData_movie.vote_average as vote_average",
    "jsonData_movie.vote_count as vote_count"
)

# Select attributes for the reviews dataframe
reviews_df = reviews_df.selectExpr(
    "jsonData_review.id as reviewId",
    "jsonData_review.media_id as movieId",
    "jsonData_review.author as author",
    "jsonData_review.content as content",
    "jsonData_review.iso_639_1 as movie_language",
    "jsonData_review.created_at as created_at",
    "jsonData_review.updated_at as updated_at",
    "jsonData_review.url as url"
)

#############################################
#             Apply transformations         #
#############################################
movie_df = movie_df.withColumn("movie_popularity", movie_df["movie_popularity"].cast(DoubleType()))
movie_df = movie_df.withColumn("vote_average", movie_df["vote_average"].cast(DoubleType()))
movie_df = movie_df.withColumn("vote_count", movie_df["vote_count"].cast(IntegerType()))
movie_df = movie_df.withColumn("release_date", movie_df["release_date"].cast(DateType()))
movie_df = movie_df.withColumn("release_date", Fun.date_format(movie_df["release_date"], "yyyy-MM-dd"))

# reviews_df = reviews_df.withColumn("movieId", movie_df.movieId)
reviews_df = reviews_df.withColumn("created_at", Fun.to_timestamp("created_at", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
reviews_df = reviews_df.withColumn("updated_at", Fun.to_timestamp("updated_at", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

# Load environment variables from .env
load_dotenv()

# Make HTTP request to retrieve genre data
genre_url = "https://api.themoviedb.org/3/genre/movie/list?language=en"
headers = {
    "accept": "application/json",
    "Authorization" : os.getenv('AUTH_KEY')
}

response = requests.get(genre_url,headers=headers)
genre_data = response.json()["genres"]

# Create SparkSession for genres
spark_genre = SparkSession.builder.getOrCreate()

# Create DataFrame from genre data
genre_df = spark_genre.createDataFrame(genre_data, schema=["genre_id", "genre_name"])

# joining movies data and genres to **flattening** json data
movie_df = movie_df.join(genre_df, on="genre_id", how="left")

###############################################################
#               Elastic Mapping and data saving               #
###############################################################
print("****************************** Insert into Elasticsearch ******************************")

from elasticsearch import Elasticsearch


# Elasticsearch connection
es = Elasticsearch([{'host': 'localhost', 'port':9200, 'scheme':'http'}])

# Define the index name
index_name1 = "movies_index"
index_name2 = "reviews_index"

# Define the mapping schema for movie data
movie_mapping = {
  "mappings": {
    "properties": {
      "movieId": {
        "type": "integer"
      },
      "title": {
        "type": "keyword"
      },
      "original_language": {
        "type": "keyword"
      },
      "overview": {
        "type": "text"
      },
      "backdrop_path": {
        "type": "text"
      },
      "poster_path": {
        "type": "text"
      },
      "media_type": {
        "type": "keyword"
      },
      "genre_name": {
        "type": "keyword"
      },
      "movie_popularity": {
        "type": "float"
      },
      "release_date": {
        "type": "date",
      },
      "vote_average": {
        "type": "float"
      },
      "vote_count": {
        "type": "integer"
      }
    }
  }
}

# Define the mapping schema for reviews
review_mapping = {
  "mappings": {
    "properties": {
      "reviewId": {
        "type": "text"
      },
      "movieId": {
        "type": "integer"
      },
      "author": {
        "type": "keyword"
      },
      "content": {
        "type": "text"
      },
      "movie_language": {
        "type": "keyword"
      },
      "created_at": {
        "type": "date",
      },
      "updated_at": {
        "type": "date",
      },
      "url": {
        "type": "text"
      }
    }
  }
}

# Create the indexes with the mapping
es.indices.create(index=index_name1, body=movie_mapping, ignore=400)
es.indices.create(index=index_name2, body=review_mapping, ignore=400)

# Insert data into Elasticsearch
def insert_data(index_name, df, checkpointlocation, _id):
    """
    'index_name' : index name in elasticsearch  \n
    'df' : Dataframe that we want to insert into elasticsearch \n
    'checkpointlocation' : To truncate the logical plan of this DataFrame \n
    '_id' : Specefiy documment _id in elasticsearch
    """
    query = df.writeStream \
        .format("org.elasticsearch.spark.sql") \
        .outputMode("append") \
        .option("es.resource", index_name) \
        .option("es.nodes", "localhost") \
        .option("es.port", "9200") \
        .option("es.mapping.id", _id) \
        .option("es.nodes.wan.only", "false") \
        .option("checkpointLocation", checkpointlocation) \
        .option("es.write.operation", "index") \
        .start()
    return query

# Insert into elasticsearch
query = insert_data(index_name1, movie_df, "../checkpoint/tmp_movies","movieId")
query = insert_data(index_name2, reviews_df, "../checkpoint/tmp_reviews","reviewId")

# Display data in console
query = movie_df.writeStream.outputMode("append").format("console").start()
query = reviews_df.writeStream.outputMode("append").format("console").start()

# # Streaming code 
# query = reviews_df.writeStream \
# .format("org.elasticsearch.spark.sql") \
# .outputMode("append") \
# .option("es.nodes", "localhost") \
# .option("es.port", "9200") \
# .option("es.resource", index_name2) \
# .option("es.mapping.id", "reviewId") \
# .option("checkpointLocation", "../checkpoint/tmp_reviews") \
# .option("es.nodes.wan.only", "false") \
# .option("es.write.operation", "index") \
# .start()

query.awaitTermination()

print("************************ Saving data done. ************************")