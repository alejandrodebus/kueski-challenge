import json
import redis
from main import app
from fastapi.testclient import TestClient
from cfg import AppCfg


client = TestClient(app)


def test_read_main():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == "Hola a tod@s"


def test_read_valid_user():
    r = redis.Redis(
        host=AppCfg.redis_host,
        port=AppCfg.redis_port,
        password=AppCfg.redis_password
    )
    features= json.dumps({
        "nb_previous_ratings": 5,
        "avg_ratings_previous": 2.2
    })
    r.set(str(99999), features)
    response = client.get("/features/user/99999")
    assert response.status_code == 200
    assert response.json() == {"nb_previous_ratings": 5, "avg_ratings_previous": 2.2}


def test_read_invalid_user():
    response = client.get("/features/user/449494949494949876")
    assert response.status_code == 404
    assert response.json() == {"detail": "User not found. Please try another user_id."}