from airflow.hooks.base import BaseHook
import psycopg2


def connect_to_db(conn_id: str):
    # Database information is stored on a json file in the conf folder
    config = BaseHook.get_connection(conn_id)

    # Psycopg2 database connection
    conn = psycopg2.connect(
        host=config.host,
        user=config.login,
        password=config.password,
        dbname=config.schema,
        port=config.port,
    )

    return conn
