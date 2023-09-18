"""
Tabela - Raw Price List Bu
"""
from airflow.models import Variable
from datetime import datetime as dt
from io import BytesIO
import pandas as pd
import boto3
import pandera as pa
from pandera import Check, Column
import numpy as np

from ..utils.postgres_methods import load_to_db
from ..utils.db_connection import connect_to_db
from ..utils.config import PriceListBU
from ..utils.cleaners import (
    count_dropped_rows_with_errors,
    clean_monetary_columns,
    count_dropped_na_rows,
    clean_pct_columns,
    fill_zeros,
)

""" File Variables """
FOLDER = "bu/price_list/"
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


def preprocess_price_list_bu(df: pd.DataFrame, latest_file_date) -> pd.DataFrame:
    """
    Series of preprocessing steps to clean original csv file.
    """
    params = PriceListBU()

    df = df.copy()

    df = count_dropped_rows_with_errors(df)
    df = count_dropped_na_rows(df)

    # Reaffirm that those columns are integers in case of wrong dtype
    # assignment on load (ex: if there is a nan, the column will turn into a float)
    df["icms"] = df["icms"].astype(int)
    df["part_number"] = df["part_number"].astype(int)
    df["part_number"] = df["part_number"].astype(str)
    df["prg"] = df["prg"].astype(int)
    df["ipi"] = df["ipi"].astype(int)
    df["ncm"] = df["ncm"].astype(int)
    df["part_contribution_margin"] = df["part_contribution_margin"].astype(int)

    # Fill part_number with less than 7 characters with left zeros
    df["part_number"] = df["part_number"].apply(
        lambda part_number: fill_zeros(str(part_number), length=7)
    )

    # Data cleaning and type definition
    df = clean_monetary_columns(df, params.MONETARY_COLUMNS_TO_CLEAN)
    df = clean_pct_columns(df, params.PCT_COLUMNS_TO_CLEAN)

    # Makes sure that the datatypes are correct
    df = df.replace(",", ".", regex=True)
    df["taxes"] = df["taxes"].astype(float)
    df["public_price"] = df["public_price"].astype(float)
    df["cofins"] = df["cofins"].astype(float)
    df["pis"] = df["pis"].astype(float)
    df["dealer_price"] = df["dealer_price"].astype(float)
    df["shelf_price"] = df["shelf_price"].astype(float)

    # Add month_year string column
    df["month_year"] = dt.combine(latest_file_date, dt.min.time())

    return df

def validate_bu_dataframe(df):

    schema_bu = pa.DataFrameSchema(
        columns={
            'part_number': Column(str),
            'part_description': Column(str),
            'part_status': Column(str),
            'public_price': Column(float, Check(lambda x: x > 0)),
            'prg': Column(int),
            'cc_dc': Column(str, Check(lambda x: any(x != '0')), coerce=True),
            'fprice_praticado': Column(float),
            'cofins': Column(float, Check(lambda x: x > 0)),
            'pis': Column(float, Check(lambda x: x > 0)),
            'ipi': Column(int, Check(lambda x: x >= 0)),
            'dealer_price': Column(float, Check(lambda x: x > 0)),
            'shelf_price': Column(float, Check(lambda x: x > 0)),
            'icms': Column(int, Check(lambda x: x > 0)),
            'serie': Column(str),
            'ncm': Column(int, Check(lambda x: x > 0)),
            'part_contribution_margin': Column(int, Check(lambda x: x >= 0)),
            'taxes': Column(float, Check(lambda x: x > 0)),
            'fprice_real': Column(float, Check(lambda x: x >= 0)),
            'fprice_real_tax': Column(float, Check(lambda x: x >= 0)),
            'margin': Column(float),
            'ipi_real': Column(float, Check(lambda x: x >= 0)),
            'piscofins_real': Column(float, Check(lambda x: x >= 0)),
            'icms_real': Column(float, Check(lambda x: x >= 0)),
            'part_cost': Column(float, Check(lambda x: x > 0)),
            'contribution_margin_real': Column(float),
            'contribution_margin_pct': Column(float),
            'dealer_net': Column(float, Check(lambda x: x > 0)),
            'shelf_30d': Column(float, Check(lambda x: x > 0)),
            'demand_12_months': Column(np.int64), #There is negative demand, that is, products that come back to BU from dealers.
            'gross_price_real': Column(float, Check(lambda x: x >= 0)),
            'forecast': Column(float),
            'shelf': Column(float),
            'gross_dolar_scania': Column(float),
            'gross_dolar_oficial': Column(float),
            'month_year': Column('datetime64[ns]')
        },
        strict=True
    )

    try:
        schema_bu.validate(df, lazy=True)
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

    latest_file_date, latest_file_name = get_last_file_from_s3(s3_client)
    max_table_date = get_last_month_year_from_db(connection, "raw_price_list_bu")

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
            header=0,  # header_row
            sep=";",
            decimal=",",
            thousands=".",
            encoding="latin-1",  # tem que trocar pra utf-8 para atualizar os meses de 11/22, 12/22 e 01/23
        )
        print(f">> {dt.now().strftime('%H:%M:%S')} File loaded as CSV: {file_path}")

        df = preprocess_price_list_bu(df, latest_file_date)
        print(f">> {dt.now().strftime('%H:%M:%S')} File preprocessed: {file_path}")
        print(f">> Processed DataFrame size: {len(df)}")

        print(">> Validating dataframe")
        validate_bu_dataframe(df)
        
        try:
            print(">> Saving data to Database")
            load_to_db(connection, df, "raw_data.raw_price_list_bu")
            print(">> Database updated")

        except Exception as error:
            print(f">> load_to_db error > {error}")

    else:
        print(">> Database already updated")

    connection.close()


if __name__ == "__main__":
    main()
