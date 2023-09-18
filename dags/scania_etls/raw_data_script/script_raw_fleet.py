"""
Tabela - Raw Fleet
"""
from airflow.models import Variable
from datetime import datetime as dt
from datetime import date
from io import BytesIO
import pandas as pd
import boto3
import pandera as pa
from pandera import Column

from ..utils.postgres_methods import load_to_db
from ..utils.db_connection import connect_to_db


""" File Variables """
FOLDER = "bu/fleet/"
S3_BUCKET_NAME = "la.aquare.data-tactics-pat-bu-brasil-external"
ACCESS_KEY = Variable.get("AWS_ACCESS_KEY_ID")
SECRET_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")


def get_last_file_from_s3(s3_client):
    """
    Fetches information from the last file inserted in s3
    """
    # Get latest file
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=FOLDER)
    latest_file = max(response["Contents"], key=lambda x: x["LastModified"])

    # Get latest file's date
    latest_file_date = latest_file["LastModified"].date()
    print(f">> S3 latest file date: {latest_file_date}")

    # Get latest file's name
    latest_file_name = latest_file["Key"].split("/")[-1]
    print(f">> S3 latest file name: {latest_file_name}")

    # Get latest file's name
    latest_file_name = latest_file["Key"].split("/")[-1]
    print(f">> S3 latest file name: {latest_file_name}")

    latest_file_date_truncated = dt.strftime(
        latest_file_date.replace(day=1), "%Y-%m-%d"
    )

    latest_file_date_day_one = dt.strptime(
        latest_file_date_truncated, "%Y-%m-%d"
    ).date()

    print(f">> S3 latest file date truncated: {latest_file_date_truncated}")

    return latest_file_date_day_one, latest_file_name


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


def process_s3_file_data(
    df: pd.DataFrame, latest_file_date, file_path: str
) -> pd.DataFrame:
    """
    Processes and standardizes file data fetched from s3
    """

    # Add month-year string column
    df["month_year"] = dt.combine(latest_file_date, dt.min.time())
    df["model"] = df["model"].str.upper()
    df["model"] = df["model"].str.extract("(^[A-Z] [0-9]{3} [A-Z]{1,2}[0-9][A-Z][0-9])")
    df = df.dropna()

    return df

def validate_fleet_dataframe(df):
    schema_fleet = pa.DataFrameSchema(
        columns={
            'nu_chassi': Column(int),
            'yr_manufact': Column(str),
            'model': Column(str),
            'month_year': Column('datetime64[ns]')
        },
        strict=True
    )

    try:
        schema_fleet.validate(df, lazy=True)
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

    # latest_file_date, latest_file_name, new_file_name = get_last_file_from_s3(s3_client)
    latest_file_date, latest_file_name = get_last_file_from_s3(s3_client)
    max_table_date = get_last_month_year_from_db(connection, "raw_fleet")

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
            sep=",",
            decimal=".",
            thousands=".",
            encoding="utf-8",
            header=0,
        )
        print(f">> {dt.now().strftime('%H:%M:%S')} File loaded as CSV: {file_path}")

        df = process_s3_file_data(df, latest_file_date, file_path)
        print(f">> {dt.now().strftime('%H:%M:%S')} File preprocessed: {file_path}")
        print(f">> Processed DataFrame size: {len(df)}")

        print(">> Validating dataframe")
        validate_fleet_dataframe(df)
        
        try:
            print(">> Saving data to Database")
            load_to_db(connection, df, "raw_data.raw_fleet")
            print(">> Database updated")

        except Exception as error:
            print(f">> load_to_db error > {error}")

    else:
        print(">> Database already updated")

    connection.close()


if __name__ == "__main__":
    main()
