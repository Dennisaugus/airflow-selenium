"""
Functions used on the Prophet task to help extracting data from Postgres
"""
from psycopg2 import sql
from typing import List
import pandas as pd
import psycopg2
import logging

from connection.postgres import PostgresConnection

LOGGER = logging.getLogger("airflow.task")


def create_df_from_cur_and_list_postgres(cur, data: List) -> pd.DataFrame:
    """
    Create a DataFrame from a cursor and list of data generated
    from Postgres.

    Args:
        cur ():
            cursor generated from the query
        data (list):
            list with data of holidays

    Returns:
        pd.DataFrame: DataFrame with holida
    """
    cols = []
    for elt in cur.description:
        cols.append(elt[0])

    # Build the DataFrame
    df_built = pd.DataFrame(data=data, columns=cols)

    LOGGER.debug(
        "DataFrame created from cursor and list of data generated from Postgres."
    )

    return df_built


def get_selected_field_info_postgres(
    field_selected: str, table: str, field_to_filter: str, field_value: str
) -> List:
    """Get info from a selected field based on a filter in a postgres table.

    Parameters
    ----------
    field_selected: str
        Field selected.
    table : str
        The table on postgres.
    field_to_filter : str
        The field name that will be used to get the id.
    field_value : str
        The field value that will be used to get the id.

    Returns
    -------
    List
        List of tuples with the results
    """
    # Connecting to Postgres
    db_postgres = PostgresConnection()

    # Build the query
    query = sql.SQL("SELECT {} FROM {} WHERE {}={}").format(
        sql.Identifier(field_selected),
        sql.Identifier(table),
        sql.Identifier(field_to_filter),
        sql.Literal(field_value),
    )

    try:
        db_postgres.cursor.execute(query)
        selected_field_info = db_postgres.cursor.fetchall()
        LOGGER.info(
            "Get %s field info from %s postgres table where %s=%s.",
            field_selected,
            table,
            field_to_filter,
            field_value,
        )

    except psycopg2.Error as error:
        LOGGER.error(error)
        selected_field_info = [(None,)]

    finally:
        db_postgres.close_connection()

    return selected_field_info


def insert_row_into_postgres(table: str, list_cols: List, list_values: List) -> int:
    """Method to insert row to a Postgres table

    Parameters
    ----------
    table : str
        Table name on Postgres
    list_cols : List
        Names of the columns that will have values inserted.
    list_values : List
        Values on the same order as the list_cols.

    Returns
    -------
    int
        id_of_new_row
    """
    # Connecting to Postgres
    db_postgres = PostgresConnection()

    # Create tuple
    tup = tuple(list_values)

    # Create the query
    query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) RETURNING id").format(
        sql.Identifier(table),
        sql.SQL(", ").join(map(sql.Identifier, list_cols)),
        sql.SQL(", ").join(sql.Placeholder() * len(list_cols)),
    )

    try:
        db_postgres.cursor.execute(query, tup)
        db_postgres.commit()
        id_of_new_row = db_postgres.cursor.fetchone()[0]
        LOGGER.info("Row inserted in Postgres table %s.", table)

    except psycopg2.Error as error:
        LOGGER.error(error)
        id_of_new_row = None
        db_postgres.rollback()

    finally:
        db_postgres.close_connection()

    return id_of_new_row


def insert_df_to_postgres(df_insert: pd.DataFrame, table: str):
    """Insert a DataFrame to a postgres table.

    Parameters
    ----------
    df_insert : pd.DataFrame
        DataFrame ready to be inserted to a table on postgres.
    table : str
        table name on postgres.
    """
    # Connecting to Postgres
    db_postgres = PostgresConnection()

    # List of DataFrame columns
    list_cols = list(df_insert.columns)

    # Create a list of tuples from the DataFrame values
    tuples = [tuple(x) for x in df_insert.to_numpy()]

    # Create query
    query = sql.SQL("INSERT INTO {} ({}) VALUES {}").format(
        sql.Identifier(table),
        sql.SQL(", ").join(map(sql.Identifier, list_cols)),
        sql.SQL(", ").join(map(sql.Literal, tuples)),
    )

    try:
        db_postgres.cursor.execute(query)
        db_postgres.commit()
        LOGGER.info("DataFrame inserted in Postgres table %s.", table)

    except psycopg2.Error as error:
        LOGGER.info(
            "ERROR %s on insert_df_to_postgres method to table %s", error, table
        )
        db_postgres.rollback()

    finally:
        db_postgres.close_connection()
