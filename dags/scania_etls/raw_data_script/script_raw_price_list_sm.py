"""
Tabela - Raw Price List SM
"""
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
from ..utils.config import PriceListSM

""" File Variables """
FOLDER = "sm/price_list/"
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


def get_header_row(obj, first_column: bytes) -> int:
    """
    Read first lines of the s3 object and return header row number
    when the first column matches.

    Args:
        obj (boto3.resources.factory.s3.Object): S3 file object

    Returns:
        int: header row number
    """
    i = 0
    buffer = 0

    # Iterate through each line of the csv file from S3 as binary strings
    for line in obj.get()["Body"].iter_lines():
        # Assume that the first line doesn't contain a cell with line break
        # and defines the amount of sep ';' as the column amount
        if i == 0:
            column_amount = str(line).count(";")

        # Counts the amount of ';' on the file
        sep_count = str(line).count(";")

        # If line doesn't contain line a line break i equals to the row
        if sep_count >= column_amount:
            row = i
            i += 1
        # In case of a cell with a line break, count of ';' will be less
        # than the total amount of columns
        elif sep_count < column_amount:
            # Increments a buffer until count of ';' is greater or equal to
            # column amount for then incrementing row and i counter
            buffer += sep_count
            if buffer == column_amount:
                row = i
                i += 1
                buffer == 0

        # Looks for the row with the first column name
        if line.startswith(first_column):
            header_row = row

    return header_row


def preprocess_price_list_sm(df: pd.DataFrame, latest_file_date) -> pd.DataFrame:
    """
    Series of preprocessing steps to clean original csv file.
    """
    params = PriceListSM()

    df = df.copy()

    df = df.rename(
        columns={
            " base_cost_real": "base_cost_real",
            "demand_last_12_months ": "demand_last_12_months",
        }
    )

    # Fill part_number with less than 7 characters with left zeros
    df["part_number"] = df["part_number"].apply(
        lambda part_number: fill_zeros(str(part_number), length=7)
    )

    # Remove R$ and transform string to float
    df = clean_monetary_columns(
        df,
        [
            "gross_price_usd",
            "base_cost_real",
            "base_cost_usd",
            "factory_price_real",
            "factory_price_usd",
            "sm_base_profit_real",
            "sm_base_profit_usd",
        ],
    )

    # Remove leading and trailing spaces from string columns
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    df["sm_base_margin"] = df["sm_base_margin"].str.strip("%").str.replace(",", ".")

    df = df.fillna(0)

    # Proper data type
    df["prg"] = df["prg"].astype(int)
    df["origin"] = df["origin"].astype(int)
    df["uso"] = df["uso"].astype(int)
    df["demand_last_12_months"] = df["demand_last_12_months"].astype(int)
    df["sm_base_margin"] = (df["sm_base_margin"].astype(float)) / 100

    rows = len(df)
    df = df.dropna(subset=params.NOT_NULL_COLS)
    rows_after_drop = len(df)
    print(f">> Dropped rows with nulls count: {rows-rows_after_drop}")

    # Add month_year string column
    df["month_year"] = dt.combine(latest_file_date, dt.min.time())

    return df

def validate_sm_dataframe(df):
    schema_sm = pa.DataFrameSchema(
        columns={
            'part_number': Column(str),
            'part_description': Column(str),
            'cc_dc': Column(str, Check(lambda x: any(x != '0')), coerce=True),
            'prg': Column(int),
            'origin': Column(int, Check(lambda x: x >= 0)),
            'uso': Column(int, Check(lambda x: x >= 0)),
            'part_status': Column(str),
            'gross_price_usd': Column(float, Check(lambda x: x >= 0)),
            'bd': Column(float, Check(lambda x: x >= 0)),
            'md': Column(float, Check(lambda x: x >= 0)),
            'dt': Column(float, Check(lambda x: x >= 0)),
            'fp_source': Column(str),
            'demand_last_12_months': Column(int, Check(lambda x: x >= 0)),
            'base_cost_real': Column(float, Check(lambda x: any(x > 0))),
            'base_cost_usd': Column(float, Check(lambda x: any(x > 0))),
            'factory_price_real': Column(float, Check(lambda x: x > 0)),
            'factory_price_usd': Column(float, Check(lambda x: x > 0)),
            'sm_base_margin': Column(float),
            'sm_base_profit_real': Column(float),
            'sm_base_profit_usd': Column(float),
            'month_year': Column('datetime64[ns]')
        },
        strict=True
    )

    try:
        schema_sm.validate(df, lazy=True)
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
    max_table_date = get_last_month_year_from_db(connection, "raw_price_list_sm")

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
        header_row = get_header_row(obj, first_column=b"part_number")

        df = pd.read_csv(
            BytesIO(data),
            header=header_row,
            sep=";",
            decimal=",",
            thousands=".",
            encoding="utf-8",
            usecols=lambda x: "Unnamed" not in x,
            na_values=["#VALUE!", "#VALUE"],
        )
        print(f">> {dt.now().strftime('%H:%M:%S')} File loaded as CSV: {file_path}")

        df = preprocess_price_list_sm(df, latest_file_date)
        print(f">> {dt.now().strftime('%H:%M:%S')} File preprocessed: {file_path}")
        print(f">> Processed DataFrame size: {len(df)}")

        print(">> Validating dataframe")
        validate_sm_dataframe(df)
        
        try:
            print(">> Saving data to Database")
            load_to_db(connection, df, "raw_data.raw_price_list_sm")
            print(">> Database updated")

        except Exception as error:
            print(f">> load_to_db error > {error}")

    else:
        print(">> Database already updated")

    connection.close()


if __name__ == "__main__":
    main()
