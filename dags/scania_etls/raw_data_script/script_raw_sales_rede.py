"""
Tabela - Raw Sales Rede
"""
from airflow.models import Variable
from datetime import datetime as dt
from io import BytesIO
import pandas as pd
import numpy as np
import boto3
import pandera as pa
from pandera import Check, Column

from ..utils.cleaners import fill_zeros, clean_monetary_columns
from ..utils.postgres_methods import load_to_db
from ..utils.db_connection import connect_to_db
from ..utils.config import SalesRede

""" File Variables """
FOLDER = "bu/sales_rede/"
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

    # latest_file_date_truncated = dt.strftime(
    #     latest_file_date.replace(day=1), "%Y-%m-%d"
    # )

    # latest_file_date_day_one = dt.strptime(latest_file_date_truncated, "%Y-%m-%d")

    # print(f">> S3 latest file date truncated: {latest_file_date_truncated}")

    # return latest_file_date_day_one, latest_file_name
    return latest_file_date, latest_file_name


def get_last_sale_date_from_db(connection, table_name: str) -> str:
    """
    Searches the most recent date in the table passed by parameter
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT TO_CHAR(max(sale_date), 'YYYY-MM-DD') AS max_sale_date
                FROM raw_data.{table_name}
            """
            )
            max_sale_date = cursor.fetchone()[0]

            max_table_date = dt.strptime(max_sale_date, "%Y-%m-%d").date()

            print(f">> Latest date from '{table_name}' table: {max_table_date}")

        return max_table_date

    except Exception as error:
        print(f">> get_last_sale_date_from_db ['{table_name}'] > {error}")

    finally:
        cursor.close()


def preprocess_sales_rede(df: pd.DataFrame) -> pd.DataFrame:
    """
    Series of preprocessing steps to clean original csv file.
    """
    params = SalesRede()

    df = df.copy()

    # Convert to a single datetime column
    df["sale_date"] = pd.to_datetime(df["sale_date"], format="%Y%m%d")

    # Fill part_number with less than 7 characters with left zeros
    df["part_number"] = df["part_number"].apply(
        lambda part_number: fill_zeros(str(part_number), length=7)
    )

    # Proper data type
    df["part_number"] = df["part_number"].astype(str)
    df["part_line"] = df["part_line"].astype(int)
    df["item_quantity"] = df["item_quantity"].astype(int)
    df["id_dealer"] = df["id_dealer"].astype(int)
    df["vl_net_billing"] = df["vl_net_billing"].astype(float)
    df["impostos"] = df["impostos"].astype(float)
    df["odometro"] = df["odometro"].astype(float)

    # Remove R$ and transform string to float
    df = clean_monetary_columns(df, ["sale_revenue"])

    # Remove leading and trailing spaces from string columns
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    # Replace None in the string format
    df["num_chassi"] = df["num_chassi"].replace("None", np.nan)

    # Get month year from sale date cause original sale date is acting a lil sus
    df["month_year"] = df["sale_date"].apply(lambda date: date.strftime("%Y-%m-01"))
    df["month_year"] = pd.to_datetime(df["month_year"])

    # Reorder columns to match table
    df = df[params.COLUMN_ORDER]

    rows = len(df)

    # Only keep rows that have 'item quantity' greater than 0
    df = df[df["item_quantity"] > 0]

    df = df.dropna(subset=params.NOT_NULL_COLS)
    rows_after_drop = len(df)
    print(
        f">> Dropped rows with nulls count or with item quantity less than one: {rows-rows_after_drop}"
    )

    return df

def validate_sales_rede_dataframe(df):

    schema_sales_rede = pa.DataFrameSchema(
        columns={
            'sale_date': Column('datetime64[ns]'),
            'month_year': Column('datetime64[ns]'),
            'part_number': Column(str),
            'part_line': Column(int, Check(lambda x: x > 0)),
            'os_type': Column(str, nullable=True),
            'sales_group': Column(str, nullable=True),
            'sale_revenue': Column(float, Check(lambda x: x >= 0)),
            'item_quantity': Column(int, Check(lambda x: x > 0)),
            'id_dealer': Column(int, Check(lambda x: x >= 0)),
            'modelo_chassi': Column(str, nullable=True),
            'ano_fabricacao': Column(float, nullable=True),
            'ds_type_customer': Column(str, nullable=True),
            'vl_net_billing': Column(float, nullable=True),
            'impostos': Column(float, nullable=True),
            'odometro': Column(float, nullable=True),
            'ctg_venda': Column(str, nullable=True),
            'cd_service_order': Column(float, nullable=True),
            'ds_city': Column(str, nullable=True),
            'ac_federative_unit': Column(str, nullable=True),
            'nu_identity_product': Column(str, nullable=True),
            'vl_nf_canceled': Column(float, nullable=True),
            'ds_status_service_order': Column(str, nullable=True),
            'ds_sale_source': Column(str, nullable=True),
            'num_chassi': Column(str, nullable=True)
        },
        strict=True
    )

    try:
        schema_sales_rede.validate(df, lazy=True)
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
    max_table_date = get_last_sale_date_from_db(connection, "raw_sales_rede")
    update_date_reference = latest_file_date - pd.DateOffset(
        days=1
    )  # The data in the file is about the sales amount from the previous day.

    print(">> Compare between dates")
    if update_date_reference.date() > max_table_date:
        print(">> Create 's3_resource' connection")
        s3_resource = boto3.resource(
            "s3", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY
        )

        file_path = FOLDER + latest_file_name
        print(">> Read file from s3")
        obj = s3_resource.Object(S3_BUCKET_NAME, file_path)
        data = obj.get()["Body"].read()

        df = pd.read_csv(BytesIO(data), sep=",", decimal=".")
        print(f">> {dt.now().strftime('%H:%M:%S')} File loaded as CSV: {file_path}")

        df = preprocess_sales_rede(df)
        print(f">> {dt.now().strftime('%H:%M:%S')} File preprocessed: {file_path}")
        print(f">> Processed DataFrame size: {len(df)}")

        print(">> Validating dataframe")
        validate_sales_rede_dataframe(df)     
        
        try:
            print(">> Saving data to Database")
            load_to_db(connection, df, "raw_data.raw_sales_rede")
            print(">> Database updated")

        except Exception as error:
            print(f">> load_to_db error > {error}")

    else:
        print(">> Database already updated")

    connection.close()


if __name__ == "__main__":
    main()
