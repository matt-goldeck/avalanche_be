from typing import Optional
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import psycopg2


class HerokuConnection(object):
    def __init__(self):
        self.connection = psycopg2.connect(os.environ.get('DATABASE_URL'), sslmode='require')
        self.cursor = self.connection.cursor()

    def close(self):
        self.cursor.close()
        self.connection.close()

    def get_tweets(self, offset=0, limit=250):
        self.cursor.execute("SELECT content, twitter_id FROM tweets ORDER BY RANDOM() DESC LIMIT %s OFFSET %s;", [limit, offset])
        return {'tweets': [{'content': res[0], 'id': res[1]} for res in self.cursor.fetchall()]}

app = FastAPI()

# Handle CORS policy
origins = [
    'http://localhost:3000'
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"Greetings from": "New Jersey"}


@app.get("/tweets/")
def read_tweets(offset: Optional[int] = 0, limit: Optional[int] = 250):
    conn = HerokuConnection()
    results = conn.get_tweets(offset=offset, limit=limit)
    conn.close()

    return results
