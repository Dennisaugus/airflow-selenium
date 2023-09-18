from airflow.models import Variable
from datetime import datetime as dt
from io import BytesIO
import pandas as pd
import boto3
import pandera as pa
from pandera import Check, Column

from ..utils.cleaners import fill_zeros, clean_monetary_columns
from ..utils.postgres_methods import load_to_db
from ..utils.db_connection import connect_to_db

""" File Variables """
FOLDER = "sm/transfer_price/"
S3_BUCKET_NAME = "la.aquare.data-tactics-pat-sales-marketing-external"
ACCESS_KEY = Variable.get("AWS_ACCESS_KEY_ID")
SECRET_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")


def get_last_file_from_s3(s3_client):
    """
    Fetches information from the last file inserted in s3
    """
    # Get latest file
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=FOLDER)
    latest_file = max(response["Contents"], key=lambda x: x["LastModified"])

    # Get latest file's name
    latest_file_name = latest_file["Key"].split("/")[-1]
    print(f">> S3 latest file name: {latest_file_name}")

    # Get latest file's date
    latest_file_date_str = (
        f'{latest_file_name.split("_")[0]}-{latest_file_name.split("_")[1]}-01'
    )
    print(f">> S3 latest file date: {latest_file_date_str}")

    latest_file_date = dt.strptime(latest_file_date_str, "%Y-%m-%d").date()

    return latest_file_date, latest_file_name


def get_last_month_year_from_db(connection, table_name: str) -> str:
    """
    Searches the most recent date in the table passed by parameter
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT TO_CHAR(max(month_year), 'YYYY-MM-DD') AS max_month_year
                FROM raw_data.{table_name}
            """
            )
            max_month_year = cursor.fetchone()[0]

            max_table_date = dt.strptime(max_month_year, "%Y-%m-%d").date()

            print(f">> Latest date from '{table_name}' table: {max_table_date}")

        return max_table_date

    except Exception as error:
        print(f">> get_last_month_year_from_db ['{table_name}'] > {error}")

    finally:
        cursor.close()


def preprocess_transfer_price_df(df: pd.DataFrame, latest_file_date) -> pd.DataFrame:
    """
    Series of preprocessing steps to clean original csv file.
    """
    df = df.copy()
    # Fill part_number with less than 7 characters with left zeros
    df["part_number"] = df["part_number"].apply(
        lambda part_number: fill_zeros(str(part_number), length=7)
    )
    df = df.drop(columns="part_description")

    # Add month-year string column
    df["month_year"] = dt.combine(latest_file_date, dt.min.time())

    return df

def validate_transfer_price_dataframe(df):
    schema_transfer_price = pa.DataFrameSchema(
        columns={
            'part_number': Column(str),
            'transfer_price': Column(float, Check(lambda x: x > 0)),
            'month_year': Column('datetime64[ns]')
        },
        strict=True
    )

    try:
        schema_transfer_price.validate(df, lazy=True)
    except pa.errors.SchemaErrors as err:
        print("Schema errors and failure cases:")
        print(err.failure_cases)
        print("\nDataFrame object that failed validation:")
        print(err.data)
        raise ValueError("Validation failed!")


def main(conn_id):
    connection = connect_to_db(conn_id=conn_id)

    print(">> Create 's3_client' connection")
    s3_client = boto3.client(
        "s3", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY
    )

    (latest_file_date, latest_file_name) = get_last_file_from_s3(s3_client)
    max_table_date = get_last_month_year_from_db(connection, "raw_transfer_price")

    print(">> Compare between dates")
    if latest_file_date > max_table_date:
        print(">> Create 's3_resource' connection")
        s3_resource = boto3.resource(
            "s3", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY
        )

        file_path = FOLDER + latest_file_name
        print(">> Read file from s3")
        obj = s3_resource.Object(S3_BUCKET_NAME, file_path)
        data = obj.get()["Body"].read()

        df = pd.read_csv(
            BytesIO(data),
            sep=";",
            header=0,
            decimal=",",
            thousands=".",
            encoding="latin-1",
            names=["part_number", "part_description", "transfer_price"],
            dtype={
                "part_number": str,
                "part_description": str,
                "transfer_price": float,
            },
        )
        print(f">> {dt.now().strftime('%H:%M:%S')} File loaded as CSV: {file_path}")

        df = preprocess_transfer_price_df(df, latest_file_date)
        print(f">> {dt.now().strftime('%H:%M:%S')} File preprocessed: {file_path}")
        print(f">> Processed DataFrame size: {len(df)}")

        print(">> Validating dataframe")
        validate_transfer_price_dataframe(df)

        try:            
            print(">> Saving data to Database")
            load_to_db(connection, df, "raw_data.raw_transfer_price")
            print(">> Database updated")

        except Exception as error:
            print(f">> load_to_db error > {error}")

    else:
        print(">> Database already updated")

    connection.close()


if __name__ == "__main__":
    main()
