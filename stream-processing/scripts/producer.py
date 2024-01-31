import time
import requests
import json
import os
from dotenv import load_dotenv
from kafka import KafkaProducer


# Kafka server address
kafka_host = "localhost:9092"  

# Topic name
movie_topic = "moviedb"
review_topic = "reviewdb"

# Load environment variables from .env
load_dotenv()

# Get the API key (Create a .env file and add API_KEY variable in it)
API_KEY = os.getenv('API_KEY')

# Page number of the endpoint
page = 1

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers=kafka_host)

print("Fetshing start.")

while True:
    MAX_RETRIES = 3
    WAIT_TIME = 5
    print("********** PAGE ***********", page)
    for retry in range(MAX_RETRIES):
        try:
            # Call the API to fetch data
            MOVIE_ENDPOINT = "https://api.themoviedb.org/3/trending/movie/day?api_key={}&language=en-US&page={}"
            response = requests.get(MOVIE_ENDPOINT.format(API_KEY, page))

            if response.status_code == 200:
                movies = response.json()["results"]
                if movies:
                    for movie in movies:
                        # Check if "id" field exists and is not empty
                        print(f"{str(movie)} \n")
                        if "id" in movie and movie["id"] is not None:
                            REVIEW_ENDPOINT = "https://api.themoviedb.org/3/movie/{}/reviews?api_key={}&language=en-US&page=1"
                            response = requests.get(REVIEW_ENDPOINT.format(int(movie["id"]), API_KEY))
                            producer.send(movie_topic, value=json.dumps(movie).encode('utf-8'))
                            if response.status_code == 200:
                                reviews = response.json()["results"]
                                # Produce the movie to Kafka
                                for review in reviews:
                                    REVIEW_ENDPOINT = "https://api.themoviedb.org/3/review/{}?api_key={}"
                                    response = requests.get(REVIEW_ENDPOINT.format(review["id"], API_KEY))
                                    if response.status_code == 200:
                                        review_detail = response.json()
                                        producer.send(review_topic, value=json.dumps(review_detail).encode('utf-8'))
                                        print(f"{str(review_detail)} \n")
                                print("\n##########################################\n")
                                # print("Send movies data to spark. Wait 5 seconds...")
                                time.sleep(5)
                        else:
                            print("Empty or missing 'id' field. Skipping sending to spark.")
                else:
                    print("Empty data. Skipping sending to spark.")
            else:
                print("Error fetching data from TMDb API")
        except requests.exceptions.RequestException as e:
            print("An error occurred while requesting reviews:", e)
        print("Retrying in", WAIT_TIME, "seconds...")
        time.sleep(WAIT_TIME)
    page = page + 1

print("Fetshing done.")