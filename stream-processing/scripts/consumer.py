import time
import findspark
import requests
findspark.init()

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType, DateType
import pyspark.sql.functions as Fun
from pyspark.conf import SparkConf
import bcrypt
SparkSession.builder.config(conf=SparkConf())

# Create a session
spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

# Read the stream comming from kafka
df = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "moviedb") \
  .load()

# Cast data as key value pair
spark_df = df.selectExpr("CAST(value AS STRING)")

# Define the schema for Actor data
actor_schema = StructType([
    StructField("adult", BooleanType(), nullable=False),
    StructField("id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("original_name", StringType(), nullable=False),
    StructField("media_type", StringType(), nullable=False),
    StructField("popularity", DoubleType(), nullable=False),
    StructField("gender", IntegerType(), nullable=False),
    StructField("known_for_department", StringType(), nullable=False),
    StructField("profile_path", StringType(), nullable=False),
    StructField("known_for", ArrayType(
        StructType([
            StructField("adult", BooleanType(), nullable=False),
            StructField("backdrop_path", StringType(), nullable=False),
            StructField("id", IntegerType(), nullable=False),
            StructField("title", StringType(), nullable=False),
            StructField("original_language", StringType(), nullable=False),
            StructField("original_title", StringType(), nullable=False),
            StructField("overview", StringType(), nullable=False),
            StructField("poster_path", StringType(), nullable=False),
            StructField("media_type", StringType(), nullable=False),
            StructField("genre_ids", ArrayType(IntegerType(), containsNull=False), nullable=False),
            StructField("popularity", DoubleType(), nullable=False),
            StructField("release_date", StringType(), nullable=False),
            StructField("video", BooleanType(), nullable=False),
            StructField("vote_average", DoubleType(), nullable=False),
            StructField("vote_count", IntegerType(), nullable=False)
        ])
    ), nullable=False)
])

# Apply schema on data
spark_df = spark_df.withColumn("jsonData", Fun.from_json(spark_df["value"], actor_schema))

# # Select our fields
# result_df = spark_df.selectExpr(
#     "jsonData.id as id",
#     "jsonData.title as title",
#     "jsonData.overview as overview",
#     "concat(jsonData.title, ' ', jsonData.overview) as description",
#     "jsonData.release_date as release_date",
#     "jsonData.popularity as popularity",
#     "jsonData.vote_average as vote_average",
#     "jsonData.original_language as original_language",
#     # "jsonData.poster_path as poster_path",
#     "explode(jsonData.genre_ids) as genre_id",
# )

select_expr = spark_df.selectExpr(
    "jsonData.id as  id_actor",
    "jsonData.name as fullname",
    "jsonData.popularity as popularity",
    "jsonData.gender as gender",
    "jsonData.known_for_department as known_for_department",
    "jsonData.profile_path as profile_path",
    "explode(known_for) as known_for",
    "jsonData.known_for.backdrop_path as backdrop_path",
    "jsonData.known_for.id as id_movie",
    "jsonData.known_for.title as title",
    "jsonData.known_for.original_language as original_language",
    "jsonData.known_for.overview as overview",
    "jsonData.known_for.poster_path as poster_path",
    "jsonData.known_for.media_type as media_type",
    "jsonData.known_for.genre_ids as genre_ids",
    "jsonData.known_for.popularity as popularity",
    "jsonData.known_for.release_date as release_date",
    "jsonData.known_for.vote_average as vote_average",
    "jsonData.known_for.vote_count as vote_count"
)

result_df = result_df.withColumn("popularity", result_df["popularity"].cast(DoubleType()))
result_df = result_df.withColumn("vote_average", result_df["vote_average"].cast(DoubleType()))
result_df = result_df.withColumn("vote_count", result_df["vote_count"].cast(IntegerType()))
result_df = result_df.withColumn("release_date", result_df["release_date"].cast(DateType()))
result_df = result_df.withColumn("release_date", Fun.date_format(result_df["release_date"], "yyyy-MM-dd"))

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
result_df = result_df.join(genre_df, on="genre_id", how="left")


print("****************************** Insert into Elasticsearch ******************************")

#Streaming code
query = result_df.writeStream \
.format("org.elasticsearch.spark.sql") \
.option("es.nodes", "localhost") \
.option("es.port", "9200") \
.option("es.resource", "movies") \
.option("checkpointLocation", "checkpoint/tmp") \
.option("es.nodes.wan.only", "false") \
.option("es.write.operation", "index") \
.option("es.spark.sql.version", "8.11") \
.start()

query.awaitTermination()

print("************************ Saving data done. ************************")