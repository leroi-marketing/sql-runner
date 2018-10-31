import psycopg2
import json

def get_connection():
    with open('auth/auth.json') as f:
        connection = psycopg2.connect(**json.load(f)['db'], connect_timeout=3)
    connection.autocommit = True
    return connection