import json

from sqlalchemy import create_engine
from sqlalchemy.engine.mock import MockConnection


def get_postgresql_connection(postgresql_secret_path: str) -> MockConnection:

    with open(f'{postgresql_secret_path}') as postgres_secret_file:
        postgres_auth = json.load(postgres_secret_file)

    username = postgres_auth['USERNAME']
    password = postgres_auth['PASSWORD']
    url = postgres_auth['URL']

    conn_string = f"postgresql://{username}:{password}@{url}"
    db_engine = create_engine(conn_string, pool_pre_ping=True)
    conn = db_engine.connect()

    return conn

