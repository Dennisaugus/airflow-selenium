"""
Tabela - Model Prediction Part Results
"""
from datetime import datetime as dt
from airflow.models import Variable
from io import BytesIO
import pandas as pd
import boto3

from ..utils.postgres_methods import load_to_db
from ..utils.db_connection import connect_to_db

""" File Variables """
FOLDER = "model_data/"
S3_BUCKET_NAME = "la.aquare.data-tactics-pat"
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
                FROM public.{table_name}
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

def main(conn_id):
    connection = connect_to_db(conn_id=conn_id)

    print(">> Create 's3_client' connection")
    s3_client = boto3.client(
        "s3", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY
    )

    latest_file_date, latest_file_name = get_last_file_from_s3(s3_client)
    max_table_date = get_last_month_year_from_db(connection, "model_prediction_part_results")

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
            dtype={
                "part_number": str,
                "demand_best_price": int,
                "demand_retail_price": int,
            },
        )
        print(f">> {dt.now().strftime('%H:%M:%S')} File loaded as CSV: {file_path}")

        df.insert(loc=1, column="model_id", value="2")

        # Rename the columns
        df = df.rename(
            columns={
                "demand_best_price": "demand_next_12_months_best_price",
                "demand_retail_price": "demand_next_12_months_retail_price",
                "revenue_best_price": "revenue_next_12_months_best_price",
                "revenue_retail_price": "revenue_next_12_months_retail_price",
                "risk": "risk_demand",
            }
        )

        print(f">> DataFrame size: {len(df)}")

        try:
            print(">> Saving data to Database")
            load_to_db(connection, df, "public.model_prediction_part_results")
            print(">> Database updated")

        except Exception as error:
            print(f">> load_to_db error > {error}")

    else:
        print(">> Database already updated")
        raise ValueError("Database already updated.")
    
    connection.close()


if __name__ == "__main__":
    main()
