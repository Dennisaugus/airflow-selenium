from tempfile import NamedTemporaryFile
from datetime import datetime as dt
import pandas as pd
import psycopg2


def load_to_db(connection, df: pd.DataFrame, table: str):
    """Insert a DataFrame to a postgres table.

    Parameters
    ----------
    connection
        Database connection.
    df : pd.DataFrame
        DataFrame ready to be inserted to a table on postgres.
    table : str
        table name on postgres.
    """
    list_cols = list(df.columns)

    try:
        with NamedTemporaryFile("w+b", suffix=".csv") as tmp:
            df.to_csv(tmp.name, index=False)

            query = ("COPY {}({})FROM STDIN DELIMITER ',' CSV HEADER").format(
                table, ",".join([str(i) for i in list_cols])
            )

            cur = connection.cursor()
            cur.copy_expert(query, tmp)
            connection.commit()

            print(
                f"{dt.now().strftime('%H:%M:%S')} - DataFrame inserted in Postgres table {table}.",
            )

    except psycopg2.Error as error:
        print(f"ERROR {error} on load_to_db method to table {table}")
        connection.rollback()

    finally:
        cur.close()
