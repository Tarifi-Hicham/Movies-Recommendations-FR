import time
import requests
import json
import os
from dotenv import load_dotenv
# from kafka import KafkaProducer


# Kafka server address
kafka_host = "localhost:9092"  
# Topic name
topic_name = "moviedb"

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
    print("********** PAGE ***********", page)
    # Call the API to fetch data
    MOVIE_ENDPOINT = "https://api.themoviedb.org/3/trending/person/day?api_key={}&language=en-US&page={}"
    response = requests.get(MOVIE_ENDPOINT.format(API_KEY, page))

    if response.status_code == 200:
        actors = response.json()["results"]
        if actors:
            for actor in actors:
                # Check if "known_for" field exists and is not empty
                if "known_for" in actor and len(actor["known_for"]) > 0:
                    # Produce the actor to Kafka
                    producer.send(topic_name, value=json.dumps(actor).encode('utf-8'))
                    # print("Send actors data to spark. Wait 5 seconds...")
                    time.sleep(5)
                else:
                    print("Empty or missing 'known_for' field. Skipping sending to spark.")
        else:
            print("Empty data. Skipping sending to spark.")
    else:
        print("Error fetching data from TMDb API")

    page = page + 1

print("Fetshing done.")