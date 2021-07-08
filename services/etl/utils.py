import json

def get_data(row):
    user = int(row['userId'])
    features = json.dumps({
        "nb_previous_ratings": row['nb_previous_ratings'],
        "avg_ratings_previous": row['avg_ratings_previous']
    })
    return user, features