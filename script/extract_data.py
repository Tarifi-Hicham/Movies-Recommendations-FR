import pandas as pd
import requests
import json
import os
from hdfs import InsecureClient
from dotenv import load_dotenv
from datetime import datetime, timedelta

def get_last_api_page(client, user_name):
    try:
        directory_path = f'/user/{user_name}'
        directories = client.list(directory_path, status=True)
        sorted_directories = sorted(directories, key=lambda x: x[1]['modificationTime'], reverse=True)
        last_directory_date = sorted_directories[0][0]
        directory_path = f'/user/{user_name}/{last_directory_date}'
        directories = client.list(directory_path, status=True)
        sorted_directories = sorted(directories, key=lambda x: x[1]['modificationTime'], reverse=True)
        last_directory_name = sorted_directories[0][0]
        if last_directory_name:
            return int(last_directory_name)
        else:
            return 0
    except:
        return 0

# Load environment variables from .env
load_dotenv()

# Get the API key (Create a .env file and add API_KEY variable in it)
API_KEY = os.getenv('API_KEY')

# Specify credentials
user_name = os.getenv('USER_NAME')
host = os.getenv('HOST')

# Connect to HDFS
client = InsecureClient(host, user=user_name)

# Get directory number of api last page data retreived
page_num = get_last_api_page(client, user_name)

if page_num and page_num >= 1:
    page = page_num + 1
else:
    page = 1

print("Fetshing start.")

start_time = datetime.now()
end_time = start_time
timer = 1

while (end_time - start_time) < timedelta(minutes=timer):
    try:
        print(f"************* PAGE {page} **************")
        # Call the API to fetch data
        MOVIE_ENDPOINT = "https://api.themoviedb.org/3/trending/person/day?api_key={}&language=en-US&page={}"
        response = requests.get(MOVIE_ENDPOINT.format(API_KEY, page))

        actors_list = []
        movies_list = []
        acted_list = []
        if response.status_code == 200:
            actors = response.json()["results"]
            if actors:
                for actor in actors:
                    # Check if "known_for" field exists and is not empty
                    if "known_for" in actor and len(actor["known_for"]) > 0:
                        # convert data details to json type
                        data_json = json.dumps(actor)
                        data = json.loads(data_json)
                        # print("data : ", data)

                        # Get more details about the actor
                        url = "https://api.themoviedb.org/3/person/{}?api_key={}&language=en-US"
                        response = requests.get(url.format(data['id'], API_KEY))
                        if response.status_code == 200:
                            actor_details = response.json()

                            # Create actor JSON object
                            actor = {
                                'actor_id': actor_details['id'],
                                'name': actor_details['name'],
                                'gender': actor_details['gender'],
                                'profile_path': actor_details['profile_path'],
                                'birthday': actor_details['birthday'],
                                'deathday': actor_details['deathday'],
                                'department': actor_details['known_for_department'],
                                'place_of_birth': actor_details['place_of_birth'],
                                'popularity': actor_details['popularity'],
                            }
                            actors_list.append(actor)

                            # Create movie JSON objects
                            for movie in data['known_for']:
                                # Get more details about the movie
                                url = "https://api.themoviedb.org/3/movie/{}?api_key={}&language=en-US"
                                response = requests.get(url.format(movie['id'], API_KEY))
                                if response.status_code == 200:
                                    movie_details = response.json()
                                
                                    movie_obj = {
                                        'movie_id': movie_details['id'],
                                        'title': movie_details['title'],
                                        'budget': movie_details['budget'],
                                        'original_language': movie_details['original_language'],
                                        'original_title': movie_details['original_title'],
                                        'overview': movie_details['overview'],
                                        'poster_path': movie_details['poster_path'],
                                        'genres': movie_details['genres'],
                                        'popularity': movie_details['popularity'],
                                        'release_date': movie_details['release_date'],
                                        'revenue': movie_details['revenue'],
                                        'vote_average': movie_details['vote_average'],
                                        'vote_count': movie_details['vote_count'],
                                        'production_companies': movie_details['production_companies'],
                                        'production_countries': movie_details['production_countries'],
                                    }
                                    movies_list.append(movie_obj)

                            # Create acted JSON objects
                            for movie in data['known_for']:
                                acted_obj = {
                                    'actor_id': actor_details['id'],
                                    'movie_id': movie['id']
                                }
                                acted_list.append(acted_obj)
                        else:
                            print("Actor details not found.")
                    else:
                        print("Empty or missing 'known_for' field. Skipping sending to DB.")
                
                print(f"Data from page {page} has been collected successfully.")

                # Generate dataframes
                df_actors = pd.DataFrame(actors_list)
                df_movies = pd.DataFrame(movies_list)
                df_acted = pd.DataFrame(acted_list)

                # HDFS files path
                actors_filepath = datetime.now().strftime('%d-%m-%Y') + f"/{page}/" + "actors.json"
                movies_filepath = datetime.now().strftime('%d-%m-%Y') + f"/{page}/" + "movies.json"
                acted_filepath = datetime.now().strftime('%d-%m-%Y') + f"/{page}/" + "acted.json"

                # Convert DataFrame to JSON format
                actors_data = df_actors.to_json(orient='records', lines=True)
                movies_data = df_movies.to_json(orient='records', lines=True)
                acted_data = df_acted.to_json(orient='records', lines=True)

                #################################
                # Upload the JSON files to HDFS #
                #################################
                with client.write(actors_filepath, overwrite=True) as hdfs_file:
                    hdfs_file.write(actors_data)

                with client.write(movies_filepath, overwrite=True) as hdfs_file:
                    hdfs_file.write(movies_data)

                with client.write(acted_filepath, overwrite=True) as hdfs_file:
                    hdfs_file.write(acted_data)

                print("Files saved into HDFS successfully.")
                print("\n##################################\n")
            else:
                print("Empty data. Skipping sending to HDFS.")
        else:
            print("Error fetching data from TMDb API, please check the api key or the endpoint url.")
    except requests.ConnectionError as e:
        print(f'ConnectionError message: {str(e)}')
        continue

    # Update end time
    end_time = datetime.now()
    page = page + 1

print(f"Fetshing done after {timer} minutes.")
