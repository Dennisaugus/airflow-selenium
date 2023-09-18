from botocore.exceptions import NoCredentialsError
from IPython.display import clear_output
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
import numpy as np
import boto3
import gc
import os

from model.price_and_margin_calculation import (
    mount_calculate_from_retail_price_response,
    func_new_dealer_net,
    func_new_bubr_margin,
    func_new_sm_margin,
)
from model.postgres_methods import (
    connect_to_db,
    etl_timeseries_all,
    current_data_all,
    median_market_price,
)
from model.model_methods import (
    dlt_model,
    demand_forecast_year,
    margin_revenue_year,
    best_price,
    percent_bad,
)

# Insert current date to run the model
CURRENT_DATE = "2023-06-01"
CURRENT_DATE_DATETIME = datetime.strptime(CURRENT_DATE, "%Y-%m-%d")
TIME_SERIES_SALES_DATE = datetime.strftime(
    CURRENT_DATE_DATETIME - pd.DateOffset(months=1), "%Y-%m-%d"
)
FORECAS_HORIZON = 12  # in months
DATE_TO_PRINT = CURRENT_DATE_DATETIME.strftime("%Y-%m")
BUCKET_NAME = "la.aquare.data-tactics-pat"

# # Loads variables present in .env
# load_dotenv()
# ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
# SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


def get_current_data_and_timeseries_model() -> pd.DataFrame:
    print("connecting...")
    connection = connect_to_db()
    print("connected!!")

    df_market = median_market_price(connection=connection)
    df_market = df_market.rename(columns={"product_code": "part_number"})
    print("median_market_price > rodou")
    # dados das peças do mês corrente
    df_current_data = current_data_all(date=CURRENT_DATE, connection=connection)
    print("current_data_all > rodou")
    df_current_data = df_current_data.merge(df_market, how="left", on="part_number")
    df_current_data["month_year"] = pd.to_datetime(
        df_current_data["month_year"], format="%Y-%m-%d"
    )

    df_timeseries = etl_timeseries_all(connection=connection)
    print("etl_timeseries_all > rodou")
    df_timeseries["month_year"] = pd.to_datetime(
        df_timeseries["month_year"], format="%Y-%m-%d"
    )

    connection.close()

    return df_current_data, df_timeseries


def run_model(df_current_data, df_timeseries):
    # lista de peças
    current_part_number = df_current_data["part_number"].unique().tolist()
    part_numbers = df_timeseries.query(
        f'month_year == "{TIME_SERIES_SALES_DATE}" & last_12months_ndemand > 5 & part_number in {current_part_number}'
    )["part_number"].unique()

    df_best_price_append = pd.DataFrame()
    for part_number in part_numbers[0:5]:
        try:
            part_number_data = df_current_data.query(f'part_number == "{part_number}"')
            prg = part_number_data["prg"].values[0]
            retail_price = part_number_data["shelf_price"].values[0]
            current_dealer_margin = part_number_data["current_dealer_margin"].values[0]
            current_sm_margin = part_number_data["current_sm_margin"].values[0]
            current_bubr_margin = part_number_data["current_bubr_margin"].values[0]
            current_factory_price = part_number_data["current_factory_price"].values[0]
            current_dealer_price = part_number_data["current_dealer_price"].values[0]
            base_cost_price = part_number_data["base_cost_price"].values[0]
            pis_percentage = part_number_data["pis_percentage"].values[0]
            icms_percentage = part_number_data["icms_percentage"].values[0]
            cofins_percentage = part_number_data["cofins_percentage"].values[0]
            ipi_percentage = part_number_data["ipi_percentage"].values[0]
            sm_base_profit_real = part_number_data[
                "sm_base_profit_real_calculated"
            ].values[0]
            bu_gross_profit_real = part_number_data[
                "bu_gross_profit_real_calculated"
            ].values[0]
            dc = part_number_data["dc"].values[0]
            market_price = part_number_data["median_market_price"].values[0]

            df_part_timeseries = df_timeseries.query(
                f'part_number == "{part_number}"'
            ).reset_index()
            price_model = dlt_model(df_timeseries=df_part_timeseries)

            # ajuste do modelo
            predicted_df = price_model.predict(
                df_part_timeseries[["month_year", "sale_unit_price"]],
                decompose=True,
                seed=8888,
            )
            predicted_df["prediction"] = abs(np.round(predicted_df["prediction"]))
            predicted_df["sale_unit_price"] = df_part_timeseries["sale_unit_price"]
            predicted_df["demand"] = df_part_timeseries["demand"]

            # resultados - melhor preço, demanda melhor preço
            part_best_price = best_price(
                part_retail_price=retail_price,
                date=CURRENT_DATE,
                periods=FORECAS_HORIZON,
                margin_revenue_year_func=margin_revenue_year,
                demand_func=demand_forecast_year,
                price_model=price_model,
                cost_func=mount_calculate_from_retail_price_response,
                current_dealer_margin=current_dealer_margin,
                current_retail_price=retail_price,
                pis_percentage=pis_percentage,
                icms_percentage=icms_percentage,
                ipi_percentage=ipi_percentage,
                cofins_percentage=cofins_percentage,
                current_factory_price=current_factory_price,
                current_sm_margin=current_sm_margin,
                current_bubr_margin=current_bubr_margin,
                base_cost_price=base_cost_price,
                dc=dc,
                median_market_price=market_price,
            )

            demand_best_price = np.round(
                demand_forecast_year(
                    price_model=price_model,
                    date=CURRENT_DATE,
                    price=part_best_price,
                    periods=FORECAS_HORIZON,
                )
            )

            demand_retail_price = np.round(
                demand_forecast_year(
                    price_model=price_model,
                    date=CURRENT_DATE,
                    price=retail_price,
                    periods=FORECAS_HORIZON,
                )
            )

            dealer_net_best_price = func_new_dealer_net(
                new_retail_price=part_best_price,
                current_dealer_margin=current_dealer_margin,
                current_retail_price=retail_price,
                pis_percentage=pis_percentage,
                icms_percentage=icms_percentage,
                ipi_percentage=ipi_percentage,
                cofins_percentage=cofins_percentage,
                current_factory_price=current_factory_price,
                current_sm_margin=current_sm_margin,
                current_bubr_margin=current_bubr_margin,
                base_cost_price=base_cost_price,
            )

            sm_margin_best_price = func_new_sm_margin(
                new_retail_price=part_best_price,
                current_dealer_margin=current_dealer_margin,
                current_retail_price=retail_price,
                pis_percentage=pis_percentage,
                icms_percentage=icms_percentage,
                ipi_percentage=ipi_percentage,
                cofins_percentage=cofins_percentage,
                current_factory_price=current_factory_price,
                current_sm_margin=current_sm_margin,
                current_bubr_margin=current_bubr_margin,
                base_cost_price=base_cost_price,
            )

            bubr_margin_best_price = func_new_bubr_margin(
                new_retail_price=part_best_price,
                current_dealer_margin=current_dealer_margin,
                current_retail_price=retail_price,
                pis_percentage=pis_percentage,
                icms_percentage=icms_percentage,
                ipi_percentage=ipi_percentage,
                cofins_percentage=cofins_percentage,
                current_factory_price=current_factory_price,
                current_sm_margin=current_sm_margin,
                current_bubr_margin=current_bubr_margin,
                base_cost_price=base_cost_price,
            )

            risk = percent_bad(
                price_model=price_model,
                date=CURRENT_DATE,
                price=part_best_price,
                demand_test=demand_retail_price,
                periods=FORECAS_HORIZON,
            )

            revenue_best_price = demand_best_price * part_best_price
            revenue_retail_price = demand_retail_price * retail_price

            data = {
                "month_year": [CURRENT_DATE],
                "dc": [dc],
                "prg": [prg],
                "part_number": [part_number],
                "best_price": [part_best_price],
                "retail_price": [retail_price],
                "market_price": [market_price],
                "demand_best_price": [demand_best_price],
                "demand_retail_price": [demand_retail_price],
                "revenue_best_price": [revenue_best_price],
                "revenue_retail_price": [revenue_retail_price],
                "risk": [risk],
            }
            df_best_price = pd.DataFrame(data=data)
            df_best_price_append = pd.concat([df_best_price_append, df_best_price])
            df_best_price_append.rename(
                columns={"best_price": "suggested_retail_price"}
            )[
                [
                    "part_number",
                    "month_year",
                    "suggested_retail_price",
                    "demand_best_price",
                    "demand_retail_price",
                    "revenue_best_price",
                    "revenue_retail_price",
                    "risk",
                ]
            ].to_csv(
                f"best_price_model_{DATE_TO_PRINT}.csv", index=False
            )
            del price_model
            del df_best_price
            gc.collect()
            clear_output(wait=False)
        except Exception as error:
            print(error)

    df_best_price_final = df_best_price_append.rename(
        columns={"best_price": "suggested_retail_price"}
    )[
        [
            "part_number",
            "month_year",
            "suggested_retail_price",
            "demand_best_price",
            "demand_retail_price",
            "revenue_best_price",
            "revenue_retail_price",
            "risk",
        ]
    ]
    return df_best_price_final


def model_result_and_other_parts(df_current_data, df_best_price_append, df_timeseries):
    part_number_list = df_current_data["part_number"].unique().tolist()
    part_number_list_model = df_best_price_append["part_number"].unique().tolist()
    pn_in_list_not_in_model = list(set(part_number_list) - set(part_number_list_model))
    df_model_not_data = df_timeseries.loc[
        (df_timeseries["part_number"].isin(pn_in_list_not_in_model))
        & (df_timeseries["month_year"] == TIME_SERIES_SALES_DATE)
    ].reset_index(drop=True)
    df_current_data_not_model = df_current_data.query(
        f"part_number not in {part_number_list_model}"
    )
    df_model_not_data.merge(
        df_current_data_not_model[["part_number", "shelf_price"]].rename(
            columns={"shelf_price": "shelf_price_next_month"}
        ),
        on="part_number",
    )["shelf_price_next_month"]
    df_model_not_data["suggested_retail_price"] = df_model_not_data.merge(
        df_current_data_not_model[["part_number", "shelf_price"]].rename(
            columns={"shelf_price": "shelf_price_next_month"}
        ),
        on="part_number",
    )["shelf_price_next_month"]
    df_model_not_data["demand_best_price"] = df_model_not_data["last_12months_qdemand"]
    df_model_not_data["demand_retail_price"] = df_model_not_data[
        "last_12months_qdemand"
    ]
    df_model_not_data["revenue_best_price"] = df_model_not_data["year_revenue"]
    df_model_not_data["revenue_retail_price"] = df_model_not_data["year_revenue"]
    df_model_not_data["month_year"] = CURRENT_DATE
    df_model_not_data["risk"] = np.nan
    df_model_not_data[
        [
            "part_number",
            "month_year",
            "suggested_retail_price",
            "demand_best_price",
            "demand_retail_price",
            "revenue_best_price",
            "revenue_retail_price",
            "risk",
        ]
    ].append(df_best_price_append).to_csv(
        f"data/best_price_all_risk_{DATE_TO_PRINT}.csv", index=False
    )
    path = f"data/best_price_all_risk_{DATE_TO_PRINT}.csv"

    return path


# def upload_file_to_s3(local_file, s3_file):
#     """
#     Uploads data present in folder 'model_data' to s3

#     Args:
#         local_file (str): Local file path
#         s3_file (str): Path of the file that will be saved in s3
#     """

#     s3 = boto3.client(
#         "s3", aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY
#     )

#     try:
#         s3.upload_file(local_file, BUCKET_NAME, s3_file)
#         print("<upload_file_to_s3> Upload successful")

#         return True

#     except FileNotFoundError:
#         print("<upload_file_to_s3> Error: file not found")

#         return False

#     except NoCredentialsError:
#         print("<upload_file_to_s3> Error: credentials not available")

#         return False


def run_model_script():
    df_current_data, df_timeseries = get_current_data_and_timeseries_model()
    df_best_price_final = run_model(df_current_data, df_timeseries)
    path_best_price_all_parts = model_result_and_other_parts(
        df_current_data, df_best_price_final, df_timeseries
    )
    print(df_best_price_final.head())
    s3_file_path = f"model_data/best_price_all_risk_{DATE_TO_PRINT}.csv"
    # upload_file_to_s3(path_best_price_all_parts, s3_file_path)
