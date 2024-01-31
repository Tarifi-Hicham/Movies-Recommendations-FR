#import flask
import pandas as pd
import json

from datetime import datetime

from flask import Flask, render_template, request
from elasticsearch import Elasticsearch

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import random



app = Flask(__name__, template_folder='templates')


def get_es_records_df():
    es_host = 'http://127.0.0.1:9200'
    es = Elasticsearch(hosts=[es_host])
    index_name = 'movies_index'

    query = {
        "query": {
            "match_all": {}
        },
        "sort": [
            {"movie_popularity": {"order": "desc"}}
        ]
    }

    result = es.search(index=index_name, body=query, size=2000)
    redata = map(lambda x: x['_source'], result['hits']['hits'])

    return pd.DataFrame(redata)

@app.route("/movies")
def get_movies():
    df = get_es_records_df()
    top_5_movies = df.head(10)
    
    return_df = pd.DataFrame(columns=['title','year'])
    return_df['movieId'] = top_5_movies['movieId']
    return_df['title'] = top_5_movies['title']
    return_df['year'] = pd.to_datetime(top_5_movies['release_date'], format='%Y-%m-%d').dt.year
    return_df['description'] = top_5_movies['overview']
    return_df['genre_name'] = top_5_movies['genre_name']
    return_df['popularity'] = top_5_movies['movie_popularity']
    return_df['ratings'] = top_5_movies['vote_average']
    return_df['vote_count'] = top_5_movies['vote_count']
    return_df['backdropPath'] = 'https://image.tmdb.org/t/p/original/' + top_5_movies['backdrop_path']
    return_df['posterPath'] = 'https://image.tmdb.org/t/p/original/' + top_5_movies['poster_path']
    
    # return json.loads(return_df.to_json(orient='records'))
    rec = json.loads(return_df.to_json(orient='records'))
    return render_template('movies.html',movies=rec)

if __name__ == '__main__':
    app.run(debug=True)