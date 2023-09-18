"""
Defines postgres connection with its airflow hook
"""
from os import environ

from airflow.hooks.postgres_hook import PostgresHook


class PostgresConnection(object):
    """
    Handles postgres hook connection
    """

    def __init__(self):
        self.__hook = PostgresHook(
            postgres_conn_id=environ["PG_CONN_ID"], schema=environ["DEFAULT_CONN_DB"]
        )
        """ Opens the connection """
        self.__connection = self.__hook.get_conn()
        """ Initiates a cursor """
        self.__cursor = self.__connection.cursor()

    def open_connection(self):
        """Opens postgres connection and initiates a cursor"""
        self.__connection = self.__hook.get_conn()
        self.__cursor = self.__connection.cursor()

    def close_connection(self):
        """Closes postgres connection and current cursor"""
        self.__connection.close()
        self.__cursor.close()

    def commit(self):
        """Make DB changes persistent"""
        self.__connection.commit()

    def rollback(self):
        """Discard current transaction"""
        self.__connection.rollback()

    @property
    def hook(self):
        """Returns postgres hook object"""
        return self.__hook

    @property
    def connection(self):
        """Returns postgres connection"""
        return self.__connection

    @property
    def cursor(self):
        """Returns postgres current cursor"""
        return self.__cursor


# Make the connection
pg_connection = PostgresConnection()
