import json
import os
from hdfs import InsecureClient
from dotenv import load_dotenv
from datetime import datetime
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import connection_db as conn
from time import sleep

#######################
# Get Data from HDFS  #
#######################

# Function to get Actor detail
def get_actors_details(actors_filepath):
    with hdfs_client.read(actors_filepath) as hdfs_file:
        actors_content = hdfs_file.read()
        json_objects = []
        for line in actors_content.splitlines():
            try:
                json_object = json.loads(line)
                json_objects.append(json_object)
            except:
                continue
        return json_objects

# Function to get movies details
def get_movies_details(movies_filepath):
    with hdfs_client.read(movies_filepath) as hdfs_file:
        movies_content = hdfs_file.read()
        json_objects = []
        for line in movies_content.splitlines():
            try:
                json_object = json.loads(line)
                json_objects.append(json_object)
            except:
                continue
        return json_objects

# Function to get movie and actor relationship
def get_acted_details(acted_filepath):
    with hdfs_client.read(acted_filepath) as hdfs_file:
        acted_content = hdfs_file.read()
        json_objects = []
        for line in acted_content.splitlines():
            try:
                json_object = json.loads(line)
                json_objects.append(json_object)
            except:
                continue
        return json_objects

# Load environment variables from .env
load_dotenv()

# Get the API key (Create a .env file and add API_KEY variable in it)
API_KEY = os.getenv('API_KEY')

# Specify credentials
user_name = 'hicham'
host = 'http://localhost:9870'

# Connect to HDFS
hdfs_client = InsecureClient(host, user=user_name)

# Specify files path directory name as today
hdfs_file_path = f"{datetime.now().strftime('%d-%m-%Y')}"

# Get directories located in today directory
directories = hdfs_client.list(hdfs_file_path, status=True)

# Get directories names to iterate them
directory_names = [directory[1]['pathSuffix'] for directory in directories if directory[1]['type'] == 'DIRECTORY']

actors = []
movies = []
acted = []
for directory in directory_names:
    # HDFS files path
    actors_filepath = hdfs_file_path + f"/{directory}/" + "actors.json"
    movies_filepath = hdfs_file_path + f"/{directory}/" + "movies.json"
    acted_filepath = hdfs_file_path + f"/{directory}/" + "acted.json"

    actors.extend(get_actors_details(actors_filepath))
    movies.extend(get_movies_details(movies_filepath))
    acted.extend(get_acted_details(acted_filepath))


# Function to execute SQL statements
def execute_sql_statement(sql):
    conn.cursor.execute(sql)
    conn.connection.commit()

###################################
# Use the Database to insert data #
###################################

# Create Database MovieDB
def use_movie_db():
    sql = ('''
        USE MovieDB;
    ''')
    execute_sql_statement(sql)

def use_movie_dm():
    sql = ('''
        USE Movie_DM;
    ''')
    execute_sql_statement(sql)

def use_actor_dm():
    sql = ('''
        USE Actor_DM;
    ''')
    execute_sql_statement(sql)


#################
# Insert Values #
#################

# Function to execute INSERT INTO TABLE statements
def execute_insert_values(sql, values):
    try:
        conn.cursor.execute(sql, values)
        conn.connection.commit()
        print("Insertion successful!")
    except conn.odbc.IntegrityError as e:
        print("Duplicate key value error!")
        print(f"Error message: {str(e)}")
        pass

# Function to insert data into the table
def insert_data_into_table(table_name, data):
    keys = data.keys()
    values = list(data.values())
    
    placeholders = ', '.join('?' * len(keys))
    columns = ', '.join(keys)
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    
    execute_insert_values(sql, values)


# Function to find an element by id
def find_element_by_id(element_list, type, id):
    for element in element_list:
        if type == 'movie' and element['movie_id'] == id:
            return element
        elif type == 'actor' and element['actor_id'] == id:
            return element
    return None

# Function to get departement info by name
def get_department_info(department_name):
    departments = {
        'Writing': {'dep_id': 1, 'name': 'Writing'},
        'Creator': {'dep_id': 2, 'name': 'Creator'},
        'Art': {'dep_id': 3, 'name': 'Art'},
        'Costume & Make-Up': {'dep_id': 4, 'name': 'Costume & Make-Up'},
        'Editing': {'dep_id': 5, 'name': 'Editing'},
        'Directing': {'dep_id': 6, 'name': 'Directing'},
        'Production': {'dep_id': 7, 'name': 'Production'},
        'Camera': {'dep_id': 8, 'name': 'Camera'},
        'Sound': {'dep_id': 9, 'name': 'Sound'},
        'Crew': {'dep_id': 10, 'name': 'Crew'},
        'Acting': {'dep_id': 11, 'name': 'Acting'},
    }
    department = departments.get(department_name)
    if department is None:
        # Add the new department to the dictionary if not exist
        new_dep_id = len(departments) + 1
        department = {'dep_id': new_dep_id, 'name': department_name}
        departments[department_name] = department
        print("New departement was added : ", departments[department_name])

    return department

# Function to get gender name by id
def get_gender_info(id):
    genders = {
        '0' : 'Not specified',
        '1' : 'Female',
        '2' : 'Male'
    }
    gender = genders.get(id)
    if not gender:
        return genders.get(0)
    return gender

# Function to encode the country name and city name to int code as id
def encode_item(name, length):
    mybytes = name.encode('utf-8')
    myint = int.from_bytes(mybytes, 'little')
    encoded_value = str(myint)[:length]
    return int(encoded_value)

# Correct the name of the country or city to be recognized
def recognize_place_of_birth(place):
    # Manual correction mapping for non-standard place names
    place_name_mapping = {
        "U.S.": "United States",
        "UK": "United Kingdom",
        "USSR (Russia)": "Russia",
        "Paddington (Circle and Hammersmith & City lines)": "Paddington"
    }
    # Manually correct the place of birth if not recognized by the geocoding service
    corrected_place_of_birth = place
    for key, value in place_name_mapping.items():
        corrected_place_of_birth = corrected_place_of_birth.replace(key, value)
    
    return corrected_place_of_birth

# Function to check if the input is city or not
def is_country(text):
    print("Checking address details wait 3 seconds..")
    sleep(3)
    geolocator = Nominatim(user_agent="my_geocoder2")
    location = geolocator.geocode(text, exactly_one=True, addressdetails=True, language='en')
    
    if location is not None and 'address' in location.raw:
        address = location.raw
        if 'country' in address and 'city' not in address:
            return True
    
    return False

# Function to get place of birth of actor using geopy library
def get_location_info(place_of_birth):
    if place_of_birth:
        max_retries = 3
        retry_count = 0
        success = False
        while retry_count < max_retries and not success:
            try:
                geolocator = Nominatim(user_agent="movie_actor_agent")
                # Retrieve location information based on place of birth
                print("start retrieving address details..")
                location = geolocator.geocode(place_of_birth, exactly_one=True, language='en')
                print("Checking address, wait 5 seconds..")
                sleep(5)
                if location is not None:
                    country = location.address.split(',')[-1].strip()
                    city = location.address.split(',')[0].strip()
                    country = recognize_place_of_birth(country)
                    city = recognize_place_of_birth(city)
                    # Here I test if the name of the city returned is correct or not
                    if not is_country(city):
                        id_country = encode_item(country, 8)
                        id_city = encode_item(city, 8)
                        if country and city:
                            return {
                                'country': {'id' : id_country, 'name' : country}, 
                                'city': {'id' : id_city, 'name' : city}
                            }
                    else:
                        id_country = encode_item(country, 8)
                        if country and city:
                            return {
                                'country': {'id' : id_country, 'name' : country}, 
                                'city': {'id' : 0, 'name' : 'None'}
                            }
                else:
                    # Split the place of birth manually if the geopy didn't recognize the address
                    place_of_birth = recognize_place_of_birth(place_of_birth)
                    country = place_of_birth.split(',')[-1].strip()
                    city = place_of_birth.split(',')[0].strip()
                    id_country = encode_item(country, 8)
                    id_city = encode_item(city, 8)
                    if country and city:
                        return {
                            'country': {'id' : id_country, 'name' : country}, 
                            'city': {'id' : id_city, 'name' : city}
                        }
            except GeocoderTimedOut as e:
                print("Geocoding timed out. Retrying...")
                sleep(2)  # Add a brief delay before retrying
            except ConnectionError as e:
                print("Failed to establish a network connection. Please check your internet connectivity.")
                continue
            retry_count += 1
        if not success:
            print("Maximum number of retries exceeded. Failed to get a successful response.")
    return None


####################################
# Prepare data for insertion in DW #
####################################


for act in acted:
    movie = find_element_by_id(movies, 'movie', act.get('movie_id'))
    actor = find_element_by_id(actors, 'actor', act.get('actor_id'))
    is_actor_inserted = False
    if movie and actor:
        # Use MovieDB DW to insert data
        use_movie_db()
        # Get movies data
        movie_object = {
            'movie_id' : movie.get('movie_id'),
            'title' : movie.get('title'),
            'original_title' : movie.get('original_title'),
            'original_language' : movie.get('original_language'),
            'overview' : movie.get('overview'),
            'poster_path' : movie.get('poster_path'),
            'release_date' : movie.get('release_date'),
        }
        insert_data_into_table('Movie', movie_object)
        # Use Movie Datamart to insert data into tables
        use_movie_dm()
        movie_fact_object = {
            'movie_id' : act.get('movie_id'),
            'movie_budget' : movie.get('budget'),
            'movie_revenue' : movie.get('revenue'),
            'movie_popularity' : movie.get('popularity'),
            'movie_vote_average' : movie.get('vote_average'),
            'movie_vote_count' : movie.get('vote_count'),
        }
        insert_data_into_table('Movie', movie_fact_object)
        insert_data_into_table('Movie_Detail', movie_object)
        # print(movie_object)
        # Add genres and the relationship between each one and its movie
        for genre in movie.get('genres'):
            genre_object = {
                'genre_id' : genre.get('id'),
                'name' : genre.get('name')
            }
            # Insert Data into DW
            use_movie_db()
            insert_data_into_table('Genre', genre_object)
            # Insert Data into DM
            use_movie_dm()
            insert_data_into_table('Genre', genre_object)
            movie_genre = {
                'movie_id' : movie.get('movie_id'),
                'genre_id' : genre.get('id')
            }
            # Insert Data into DW
            use_movie_db()
            insert_data_into_table('Movie_Genre', movie_genre)
            # Insert Data into DM
            use_movie_dm()
            insert_data_into_table('Movie_Genre', movie_genre)
            # print(genre)
        # Add production companies and the relationship between each one and its movie
        for company in movie.get('production_companies'):
            production_company = {
                'prod_company_id' : company.get('id'),
                'name' : company.get('name'),
                'logo_path' : company.get('logo_path'),
                'origin_country' : company.get('origin_country'),
            }
            # Insert Data into DW
            use_movie_db()
            insert_data_into_table('Production_Company', production_company)
            # Insert Data into DM
            use_movie_dm()
            insert_data_into_table('Production_Company', production_company)
            movie_prod_company = {
                'movie_id' : movie.get('movie_id'),
                'prod_company_id' : company.get('id')
            }
            # Insert Data into DW
            use_movie_db()
            insert_data_into_table('Movie_Prod_Company', movie_prod_company)
            # Insert Data into DM
            use_movie_dm()
            insert_data_into_table('Movie_Prod_Company', movie_prod_company)
            # print(str(movie_prod_company) + "\n" + str(production_company))
        # Add production countries and the relationship between each one and its movie
        for country in movie.get('production_countries'):
            production_country = {
                'prod_country_id' : country.get('iso_3166_1'),
                'name' : country.get('name')
            }
            # Insert Data into DW
            use_movie_db()
            insert_data_into_table('Production_Country', production_country)
            # Insert Data into DM
            use_movie_dm()
            insert_data_into_table('Production_Country', production_country)
            movie_prod_country = {
                'movie_id' : movie.get('movie_id'),
                'prod_country_id' : country.get('iso_3166_1')
            }
            # Insert Data into DW
            use_movie_db()
            insert_data_into_table('Movie_Prod_Country', movie_prod_country)
            # Insert Data into DM
            use_movie_dm()
            insert_data_into_table('Movie_Prod_Country', movie_prod_country)
            # print(str(movie_prod_country) + "\n" + str(production_country))
                
        if not is_actor_inserted:
            dep = get_department_info(actor.get('department'))
            department = {
                'dep_id' : dep.get('dep_id'),
                'name' : dep.get('name'),
            }
            # Insert Data into DW
            use_movie_db()
            insert_data_into_table('Department', department)
            # Insert Data into DM
            use_actor_dm()
            insert_data_into_table('Department', department)
            # print(department)
            gender = {
                'gender_id' : actor.get('gender'),
                'name' : get_gender_info(str(actor.get('gender'))),
            }
            # Insert Data into DW
            use_movie_db()
            insert_data_into_table('Gender', gender)
            # Insert Data into DM
            use_actor_dm()
            insert_data_into_table('Gender', gender)
            # print(gender)
            # print(actor.get('place_of_birth'))
            location_info = get_location_info(actor.get('place_of_birth'))
            if location_info:
                country = location_info.get('country')
                city = location_info.get('city')
                # Create country and city objects
                country_object = {
                    'country_id' : country.get('id'),
                    'name' : country.get('name'),
                }
                city_object = {
                    'city_id' : city.get('id'),
                    'name' : city.get('name'),
                }
                
            else:
                country_object = {
                    'country_id' : 0,
                    'name' : 'None',
                }
                city_object = {
                    'city_id' : 0,
                    'name' : 'None',
                }
            ##########################
            #   Insert Countries     #    
            ##########################
            # Insert Data into DW
            use_movie_db()
            insert_data_into_table('Country', country_object)
            # Insert Data into DM
            use_actor_dm()
            insert_data_into_table('Country', country_object)
            ##########################
            #   Insert Cities        #    
            ##########################
            # Insert Data into DW
            use_movie_db()
            insert_data_into_table('City', city_object)
            # Insert Data into DM
            use_actor_dm()
            insert_data_into_table('City', city_object)
            # print(actor.get('place_of_birth'))
            # print(country_object)
            # print(city_object)
            # print("\n")
            actor_object = {
                'actor_id' : actor.get('actor_id'),
                'gender_id' : gender.get('gender_id'),
                'dep_id' : department.get('dep_id'),
                'country_id' : country_object.get('country_id'),
                'city_id' : city_object.get('city_id'),
                'full_name' : actor.get('name'),
                'profile_path' : actor.get('profile_path'),
                'birthday' : actor.get('birthday'),
                'deathday' : actor.get('deathday')
            }
            # Insert Data into DW
            use_movie_db()
            insert_data_into_table('Actor', actor_object)
            actor_dm_object = {
                'actor_id' : actor.get('actor_id'),
                'gender_id' : gender.get('gender_id'),
                'dep_id' : department.get('dep_id'),
                'country_id' : country_object.get('country_id'),
                'city_id' : city_object.get('city_id'),
                'actor_popularity' : actor.get('popularity'),
            }
            actor_detail_dm_object = {
                'actor_id' : actor.get('actor_id'),
                'full_name' : actor.get('name'),
                'profile_path' : actor.get('profile_path'),
                'birthday' : actor.get('birthday'),
                'deathday' : actor.get('deathday')
            }
            # Insert Data into DM
            use_actor_dm()
            insert_data_into_table('Actor', actor_dm_object)
            insert_data_into_table('Actor_Detail', actor_detail_dm_object)
            # print(actor_object)
            # print("\n")
            is_actor_inserted = True
        
        realization_object = {
            'actor_id' : act.get('actor_id'),
            'movie_id' : act.get('movie_id'),
            'actor_popularity' : actor.get('popularity'),
            'movie_budget' : movie.get('budget'),
            'movie_revenue' : movie.get('revenue'),
            'movie_popularity' : movie.get('popularity'),
            'movie_vote_average' : movie.get('vote_average'),
            'movie_vote_count' : movie.get('vote_count'),
        }
        # Insert Data into DW
        use_movie_db()
        insert_data_into_table('Realization', realization_object)
        # print(realization_object)

        print("Data inserted successfuly into Datawarehouse and Datamarts.")

print("Insertion process done.")

print("Terminating connection to SQL Server.")
conn.connection.close()


##################
#   Indexation   #
##################

# CREATE INDEX [PK/FK] ON  [Movie_Prod_Company] ([movie_id], [prod_company_id]);

# CREATE INDEX [PK/FK] ON  [Movie_Prod_Country] ([movie_id], [prod_country_id]);

# CREATE INDEX [PK/FK] ON  [Movie_Genre] ([movie_id], [genre_id]);

# CREATE INDEX [PK/FK] ON  [realization] ([actor_id], [movie_id]);
