from airflow.hooks.base import BaseHook
from datetime import datetime
import psycopg2

from scrapers.config.stores_info import FILE_NAME_DICT


def load_to_db(
    connection, file_name: str, table: str = "raw_data.raw_competitors_products"
):
    """Insert a DataFrame to a postgres table.

    Parameters
    ----------
    connection
        Database connection.
    df : str
        CSV file ready to be inserted to a table on postgres.
    table : str
        table name on postgres.
    """
    print(file_name)
    try:
        with open(file_name, "r+") as tmp:
            query = """
                COPY raw_data.raw_competitors_products
                    (job_datetime, product_code, id_competitor, product_name, product_details, 
                    product_price, product_link, product_image_link, product_info)
                FROM STDIN DELIMITER ',' CSV HEADER
            """

            cur = connection.cursor()
            cur.copy_expert(query, tmp)
            connection.commit()

        print(
            f"{datetime.now().strftime('%H:%M:%S')} - DataFrame inserted in Postgres table {table}.",
        )

    except psycopg2.Error as error:
        print(f"ERROR {error} on load_to_db method to table {table}")
        connection.rollback()

    finally:
        cur.close()


def connection(conn_id: str):
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


def main(conn_id):
    db_conn = connection(conn_id=conn_id)
    date = datetime.today().strftime("%Y-%m-%d")

    # Performs the copy operation from local repository to the database for each file present in the 'FILE_NAME_DICT' dictionary
    for value in FILE_NAME_DICT.values():
        # Set the local file path
        file_name = date + value + ".csv"
        local_file_name = "scrapers_data/" + file_name

        try:
            load_to_db(db_conn, local_file_name)
        except:
            print(f"File {local_file_name} not found!")
            pass


if __name__ == "__main__":
    main()
