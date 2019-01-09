import psycopg2
from snowflake import connector
import json
from types import SimpleNamespace

with open('auth/config.json')as f:
    config = SimpleNamespace(**json.load(f))


def get_connection():
    if config.database_type == 'snowflake':
        connection = connector.connect(**config.auth, connect_timeout=3)
    elif config.database_type == 'redshift' or config.database_type == 'postgres':
        connection = psycopg2.connect(**config.auth, connect_timeout=3)
    else:
        raise ValueError('Invalid database: {}! Value must be snowflake, redshift or postgres'.format(config.database))
    connection.autocommit = True
    return connection
