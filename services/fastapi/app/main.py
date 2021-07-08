from fastapi import FastAPI, HTTPException
from kueski_store import KueskiStore

app = FastAPI()

store = KueskiStore()


@app.get("/")
def home():
    return "Hola a tod@s"

@app.get("/ping")
def home():
    return "Pong!"


@app.get("/features/user/{user_id}")
async def get_features_user_id(user_id):
    request_data = store.get(user_id)
    if not request_data:
        raise HTTPException(status_code=404, detail="User not found. Please try another user_id.")
    return request_data