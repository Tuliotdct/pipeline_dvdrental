from aws_secrets import get_secret
from sqlalchemy import create_engine
from sqlalchemy.engine import URL


def get_connection():

    secret = get_secret()

    conn = URL.create(
        drivername='postgresql+psycopg2',
        username = secret['username'],
        password = secret['password'],
        host = secret['host'],
        port = secret['port'],
        database = secret['dbname']
    )

    engine = create_engine(conn)

    return engine
