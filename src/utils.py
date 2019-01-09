import json
from types import SimpleNamespace

with open('auth/config.json')as f:
    config = SimpleNamespace(**json.load(f))


def get_connection():
    if config.database_type == 'snowflake':
        import snowflake.connector
        connection = snowflake.connector.connect(**config.auth)
    elif config.database_type == 'redshift' or config.database_type == 'postgres':
        import psycopg2
        connection = psycopg2.connect(**config.auth, connect_timeout=3)
    else:
        raise ValueError('Invalid database: {}! Value must be snowflake, redshift or postgres'.format(config.database))
    connection.autocommit = True
    return connection
