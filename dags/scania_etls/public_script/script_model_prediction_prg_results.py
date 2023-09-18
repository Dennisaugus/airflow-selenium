"""
Tabela - Best Price Index
"""
from sklearn.preprocessing import PowerTransformer
import pandas as pd
import numpy as np
from datetime import datetime as dt

from ..utils.postgres_methods import load_to_db
from ..utils.db_connection import connect_to_db


def get_last_month_year_from_db(connection, table_name: str) -> tuple:
    """
    Searches the most recent date in the table passed by parameter
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT TO_CHAR(max(data_month_year), 'YYYY-MM-DD') AS max_data_month_year
                FROM public.{table_name}
            """
            )
            max_data_month_year = cursor.fetchone()[0]

            insertion_date = dt.strftime(dt.strptime(max_data_month_year, "%Y-%m-%d") + pd.DateOffset(months=1), "%Y-%m-%d")

            print(f">> Latest date from '{table_name}' table: {max_data_month_year}")
            print(f">> Date to be inserted in '{table_name}' table: {insertion_date}")

        return max_data_month_year, insertion_date

    except Exception as error:
        print(f">> get_last_month_year_from_db ['{table_name}'] > {error}")

    finally:
        cursor.close()

def etl_timeseries_prediction(connection, date: str) -> pd.DataFrame:
    """Query to get the dataset from scania_parts_tab"""
    sql = """
        SELECT
        DATE_TRUNC('month', insertion_date)::DATE AS insertion_date,
        prg, 
        cc,
        dc,
        SUM( COALESCE(demand_next_12_months_retail_price, 0) ) AS demand_next_12_months_retail_price,
        SUM( COALESCE(demand_next_12_months_best_price, 0) ) AS demand_next_12_months_best_price,
        SUM( COALESCE(sm_base_profit_real_calculated * demand_next_12_months_retail_price, 0) ) AS profit_sm_next_12_months_retail_price,
        SUM( COALESCE((suggested_factory_price - base_cost_price) * demand_next_12_months_best_price, 0) ) AS profit_sm_next_12_months_best_price,
        SUM( COALESCE(bu_gross_profit_real_calculated * demand_next_12_months_retail_price, 0) ) AS profit_bu_next_12_months_retail_price,
        SUM( COALESCE((suggested_dealer_net_price - suggested_factory_price) * demand_next_12_months_best_price, 0) ) AS profit_bu_next_12_months_best_price
        FROM scania_parts_tab
        where insertion_date = '{}'
        GROUP BY prg, insertion_date, cc, dc
    """.format(
        date
    )
    df = pd.read_sql(sql=sql, con=connection)

    return df


def get_and_calculate_best_price_index(
    connection, insertion_date: str, data_month_year: str
) -> pd.DataFrame:
    df_prediction = etl_timeseries_prediction(connection, date=insertion_date)
    df_prediction["prg"] = df_prediction["prg"].astype(str)
    df_prediction["best_price_index"] = (
        df_prediction["profit_sm_next_12_months_best_price"]
    ) + (df_prediction["profit_bu_next_12_months_best_price"])
    df_prediction["data_month_year"] = pd.to_datetime(data_month_year)
    df_prediction = df_prediction[
        ["prg", "insertion_date", "data_month_year", "best_price_index"]
    ]

    return df_prediction


def map_to_gaussian(df_prediction: pd.DataFrame) -> np.array:
    # https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.PowerTransformer.html#sklearn.preprocessing.PowerTransformer
    # https://scikit-learn.org/stable/modules/preprocessing.html
    pt = PowerTransformer(method="yeo-johnson", standardize=True)

    return pt.fit_transform(df_prediction["best_price_index"].values.reshape(-1, 1))


def categorize(best_price_index_scaling: pd.Series) -> pd.Series:
    bins = [-np.inf, -2, 0, 2, 3, np.inf]
    labels = ["Very Low", "Low", "Normal", "High", "Very High"]

    return pd.cut(best_price_index_scaling, bins=bins, labels=labels)


def main(conn_id):
    connection = connect_to_db(conn_id=conn_id)

    max_data_month_year, insertion_date = get_last_month_year_from_db(connection, "model_prediction_prg_results")

    # 1 - Calculate the best price index based on the sum of the prediction profits of sm and bu for the next 12 months
    df_prediction = get_and_calculate_best_price_index(
        connection, insertion_date, max_data_month_year
    )

    # 2 - Map the best_price_index to a Gaussian distribution
    df_prediction["best_price_index_scaling"] = map_to_gaussian(df_prediction)

    # 3 - Categorize the data based on how far in standard deviations the best_price_index is from the mean of the 'gaussian' distribution
    df_prediction["category"] = categorize(df_prediction.best_price_index_scaling)

    # 4 - Prepare the dataframe summary and export the Best Price Index per PRG and per *-data_month_year.csv
    df_prediction.sort_values("best_price_index_scaling", ascending=False, inplace=True)
    df_prediction.reset_index(drop=True, inplace=True)

    # 5 - Rename columns to match the table that is in the database
    df_prediction.rename(
        columns={
            "best_price_index": "best_price_index_percentage",
            "category": "best_price_index_category",
        },
        inplace=True,
    )

    # 6 - Load to DB
    try:
        load_to_db(connection, df_prediction, "public.model_prediction_prg_results")

    except Exception as error:
        print(f"Error > {error}")

    finally:
        connection.close()


if __name__ == "__main__":
    main()
