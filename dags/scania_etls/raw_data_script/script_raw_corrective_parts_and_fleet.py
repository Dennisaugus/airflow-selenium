"""
Tabela - Raw Corrective Parts and Fleet
"""
from airflow.models import Variable
from datetime import datetime as dt
from io import BytesIO
import pandas as pd
import numpy as np
import boto3

from ..utils.postgres_methods import load_to_db
from ..utils.db_connection import connect_to_db


""" File Variables """
FOLDER = "Ciclo Vida Peças/"
FILE_NAME = "Repairs per chassis with Vehicle type_2.csv"
FILE_PATH = FOLDER + FILE_NAME
S3_BUCKET_NAME = "la.aquare.data-tactics-pat-bu-brasil-external"
ACCESS_KEY = Variable.get("AWS_ACCESS_KEY_ID")
SECRET_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")


def fleet_query(connection):
    sql = "SELECT * FROM view_fleet"
    df = pd.read_sql(sql=sql, con=connection)

    return df


def raw_repair(file_path):
    s3 = boto3.resource(
        "s3", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY
    )
    bucket = s3.Bucket(S3_BUCKET_NAME)
    print(">> Inside Bucket")

    # Search s3 file by its name and return boolean flag
    s3_file_exists = lambda filename: bool(list(bucket.objects.filter(Prefix=filename)))

    start_time = dt.now()
    print(">> Starting timer...")

    if s3_file_exists(file_path):
        print(">> File Exists")

        obj = s3.Object(S3_BUCKET_NAME, file_path)
        data = obj.get()["Body"].read()

        df = pd.read_csv(
            BytesIO(data),
            sep=";",
            header=0,
            decimal=",",
            names=[
                "part_number_desc",
                "part_number",
                "model",
                "1y",
                "2y",
                "3y",
                "4y",
                "5y",
            ],
            dtype={"part_number": str},
        )
        print(f">> {dt.now().strftime('%H:%M:%S')} File loaded as CSV: {file_path}")
        print(f">> {dt.now().strftime('%H:%M:%S')} File preprocessed: {file_path}")

    end_time = dt.now()
    print(f">> Finished in {end_time - start_time}")

    return df


def preprocess_repair(df: pd.DataFrame) -> pd.DataFrame:
    df_repair = df.copy()
    df_repair = df_repair.fillna(0)
    df_repair["part_number"] = df_repair["part_number"].str.pad(width=7, fillchar="0")
    df_repair = df_repair.melt(
        id_vars=["part_number_desc", "part_number", "model"],
        value_vars=["1y", "2y", "3y", "4y", "5y"],
        var_name="year_model",
        value_name="repair_perc",
    )
    df_repair["model"] = df_repair["model"].str.upper()
    df_repair["model"] = df_repair["model"].str.extract(
        "(^[A-Z] [0-9]{3} [A-Z]{1,2}[0-9][A-Z][0-9])"
    )
    df_repair["year_model"] = df_repair["year_model"].str.replace(r"\D", "", regex=True)
    df_repair['year_model'] = df_repair['year_model'].astype('int')

    return df_repair


def corrective_parts_and_fleet(df: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    df_market_size = df.copy()
    df_market_size = df.merge(df2, on=["model", "year_model"])
    df_market_size["market_size"] = (
        df_market_size["quantity"] * df_market_size["repair_perc"]
    )

    grouped = df_market_size[["market_size", "part_number"]].groupby("part_number")
    df_market_grouped = (
        pd.DataFrame({"market_size": grouped["market_size"].apply(np.sum)})
        .reset_index()
        .sort_values("market_size", ascending=False)
    )
    df_market_grouped["market_size"] = df_market_grouped["market_size"].astype(int)

    return df_market_grouped


def main(conn_id):
    connection = connect_to_db(conn_id=conn_id)

    print(f">> file_path > {FILE_PATH}")
    df_repair = raw_repair(FILE_PATH)
    df_fleet = fleet_query(connection)
    df_repair_preprocessed = preprocess_repair(df_repair)
    df_corrective_parts_and_fleet = corrective_parts_and_fleet(
        df_fleet, df_repair_preprocessed
    )
    df_corrective_parts_and_fleet["month_year"] = (
        dt.today().replace(day=1).strftime("%Y-%m-%d")
    )

    try:
        load_to_db(
            connection,
            df_corrective_parts_and_fleet,
            "raw_data.raw_corrective_parts_and_fleet",
        )

    except Exception as error:
        print(f">> Error: {error}")

    finally:
        connection.close()


if __name__ == "__main__":
    main()
