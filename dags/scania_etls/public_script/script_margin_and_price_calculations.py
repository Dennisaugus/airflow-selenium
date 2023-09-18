"""
Tabela - Margin and Price Calculations
"""
from datetime import datetime as dt
import pandas as pd
import numpy as np

from ..utils.postgres_methods import load_to_db
from ..utils.db_connection import connect_to_db


""" File Variables """
BIPHASIC_TAX = 0.0925
PIS_NON_BIPHASIC = 0.023
MINIMUM_MARGINS_SM = {"min1": 0.375, "min2": 0.153}
MINIMUM_MARGINS_BUBR = {"min1": 0.24, "min2": 0.05}

def get_last_month_year_from_db(connection, table_name: str) -> str:
    """
    Searches the most recent date in the table passed by parameter
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT TO_CHAR(max(insertion_date) + interval '1 month' , 'YYYY-MM-DD') AS max_insertion_date
                FROM public.{table_name}
            """
            )
            max_insertion_date = cursor.fetchone()[0]

            max_table_date = dt.strptime(max_insertion_date, "%Y-%m-%d").date()

            print(f">> Date to be inserted in '{table_name}' table: {max_table_date}")

        return max_table_date

    except Exception as error:
        print(f">> get_last_month_year_from_db ['{table_name}'] > {error}")

    finally:
        cursor.close()

# Select what we need from table [price_list/time_series]
def extract_from_price_list_and_time_series(connection, max_table_date):
    print("Query DB")
    # CONFERIR O VALOR DO DOLAR NO RRP_REAL
    sql = f"""
        SELECT 
            pl.part_number, 
            pl.prg, 
            pl.cc, 
            pl.dc, 
            pl.month_year AS insertion_date, 
            pl.part_description, 
            pl.base_cost_price,
            pl.current_factory_price,
            pl.current_dealer_price,
            pl.current_retail_price,
            pl.current_sm_margin::float, 
            pl.current_bubr_margin::float, 
            pl.current_dealer_margin::float,
            ts.percentage_in_revenue::float,
            (pl.ipi_percentage::float)/100 AS ipi_percentage, 
            pl.icms_percentage::float/100 AS icms_percentage,
            round(pl.pis_percentage::numeric/100,4) AS pis_percentage, 
            pl.cofins_percentage::float/100 AS cofins_percentage,
            pl.current_dealer_net_price::float,
            ts.demand,
            rrp_real::float,
            ts.revenue::float,
            ts.revenue_diff_last_month::float,
            pl.sm_base_profit_real_calculated::float,
            pl.bu_gross_profit_real_calculated::float,
            ts.sales_unit_price::float,
            ts.demand_last_12_months
        FROM price_list pl
        LEFT JOIN time_series ts 
            ON pl.part_number = ts.part_number 
            AND pl.month_year = ts.month_year + interval '1 month'
        --INNER JOIN (SELECT MAX(month_year) AS max_month_year FROM price_list) AS my
            --ON pl.month_year = my.max_month_year  
        WHERE pl.month_year = '{max_table_date}'
        """

    print("End Query DB")
    df_scania_products = pd.read_sql(sql=sql, con=connection)

    return df_scania_products


def extract_from_model_prediction(connection, max_table_date):
    sql = f"""
        SELECT 
            mp.part_number,
            mp.month_year,
            mp.suggested_retail_price,
            mp.demand_next_12_months_best_price,
            mp.demand_next_12_months_retail_price,
            mp.revenue_next_12_months_best_price,
            mp.revenue_next_12_months_retail_price
        FROM model_prediction_part_results mp
        WHERE mp.model_id = 2
        AND mp.month_year = '{max_table_date}'
        """

    df_scania_products = pd.read_sql(sql=sql, con=connection)

    return df_scania_products


def calculate_all_prices_and_margins_changes(
    base_cost_price: float,
    current_factory_price: float,
    new_dealer_price: float,
    retail_price_percentage_diff: float,
    taxes: dict,
    share_discount: bool = False,
) -> tuple:
    """Calculates the new_factory_price, new_sm_margin, new_bubr_margin"""
    pis_tax = taxes["pis"]
    icms_tax = taxes["icms"]
    cofins_tax = taxes["cofins"]

    if share_discount:
        new_factory_price = current_factory_price * (
            1 + (retail_price_percentage_diff * 0.5)
        )
    else:
        new_factory_price = current_factory_price * (1 + retail_price_percentage_diff)

    new_sm_margin = 1 - base_cost_price / new_factory_price
    new_bubr_margin = 1 - (
        new_factory_price / (new_dealer_price * (1 - pis_tax - cofins_tax - icms_tax))
    )

    return (new_sm_margin, new_factory_price, new_bubr_margin)


def calculate_bubr_margin(
    current_factory_price: float,
    current_sm_margin: float,
    new_dealer_price: float,
    taxes: dict,
) -> tuple:
    """Calculates the new_bubr_margin"""
    pis_tax = taxes["pis"]
    icms_tax = taxes["icms"]
    cofins_tax = taxes["cofins"]

    # The values ​​of new_factory_price and new_sm_margin remain the same as current
    new_factory_price = current_factory_price
    new_sm_margin = current_sm_margin

    new_bubr_margin = 1 - (
        new_factory_price / (new_dealer_price * (1 - pis_tax - cofins_tax - icms_tax))
    )

    return (new_sm_margin, new_factory_price, new_bubr_margin)


def mount_calculate_from_retail_price_response(
    part_number,
    suggested_retail_price,
    base_cost_price,
    current_factory_price,
    current_retail_price,
    current_sm_margin,
    current_bubr_margin,
    current_dealer_margin,
    ipi_tax,
    icms_tax,
    pis_tax,
    cofins_tax,
    demand_next_12_months_retail_price,
    sm_base_profit_real_calculated,
    bu_gross_profit_real_calculated,
    demand_next_12_months_best_price,
    current_dealer_net_price,
):
    """
    Mounts calculate_from_retail_price endpoint response

    Args:
        request_data (dict): calculate_from_retail_price request
            Format: {
                "parts_id": int,
                "new_retail_price": float,
            }

    Returns:
        response (dict): calculate_from_retail_price response
            Format: {
                "new_breakeven_point": int,
                "new_factory_price": float,
                "new_dealer_net_price": float,
                "new_dealer_price": float,
                "new_retail_price": float,
                "new_sm_margin": float,
                "new_bubr_margin": float,
                "new_dealer_margin": float,
            }
    """

    taxes = {
        "ipi": ipi_tax,
        "icms": icms_tax,
        "pis": pis_tax,
        "cofins": cofins_tax,
    }

    new_retail_price = suggested_retail_price

    # Dealer's margin remains the same
    new_dealer_margin = current_dealer_margin

    # Retail price discount or addition percentage
    retail_price_percentage_diff = (new_retail_price / current_retail_price) - 1

    if pis_tax == PIS_NON_BIPHASIC:
        new_dealer_price = (new_retail_price * (1 - new_dealer_margin - icms_tax)) / (
            1 + ipi_tax - icms_tax
        )
    else:
        new_dealer_price = (
            new_retail_price * (1 - new_dealer_margin - icms_tax - BIPHASIC_TAX)
        ) / (1 + ipi_tax - icms_tax - BIPHASIC_TAX)

    new_dealer_net_price = new_dealer_price * (1 - (icms_tax + pis_tax + cofins_tax))
    user_group = "sm"
    if user_group == "sm":
        if (
            (
                current_sm_margin >= MINIMUM_MARGINS_SM["min1"]
                and current_bubr_margin >= MINIMUM_MARGINS_BUBR["min1"]
            )
            or (
                current_sm_margin < MINIMUM_MARGINS_SM["min2"]
                and current_bubr_margin < MINIMUM_MARGINS_BUBR["min2"]
            )
            or (
                (
                    current_sm_margin < MINIMUM_MARGINS_SM["min1"]
                    and current_bubr_margin < MINIMUM_MARGINS_BUBR["min1"]
                )
                and (
                    current_sm_margin >= MINIMUM_MARGINS_SM["min2"]
                    and current_bubr_margin >= MINIMUM_MARGINS_BUBR["min2"]
                )
            )
        ):
            (
                new_sm_margin,
                new_factory_price,
                new_bubr_margin,
            ) = calculate_all_prices_and_margins_changes(
                base_cost_price,
                current_factory_price,
                new_dealer_price,
                retail_price_percentage_diff,
                taxes,
                share_discount=True,
            )

        elif current_sm_margin >= MINIMUM_MARGINS_SM["min1"]:  # Caso 2
            if retail_price_percentage_diff < 0:
                (
                    new_sm_margin,
                    new_factory_price,
                    new_bubr_margin,
                ) = calculate_all_prices_and_margins_changes(
                    base_cost_price,
                    current_factory_price,
                    new_dealer_price,
                    retail_price_percentage_diff,
                    taxes,
                )
            else:
                (
                    new_sm_margin,
                    new_factory_price,
                    new_bubr_margin,
                ) = calculate_bubr_margin(
                    current_factory_price,
                    current_sm_margin,
                    new_dealer_price,
                    taxes,
                )

        elif current_bubr_margin >= MINIMUM_MARGINS_BUBR["min1"]:
            if retail_price_percentage_diff < 0:
                (
                    new_sm_margin,
                    new_factory_price,
                    new_bubr_margin,
                ) = calculate_bubr_margin(
                    current_factory_price,
                    current_sm_margin,
                    new_dealer_price,
                    taxes,
                )
            else:
                (
                    new_sm_margin,
                    new_factory_price,
                    new_bubr_margin,
                ) = calculate_all_prices_and_margins_changes(
                    base_cost_price,
                    current_factory_price,
                    new_dealer_price,
                    retail_price_percentage_diff,
                    taxes,
                )

        elif current_sm_margin < MINIMUM_MARGINS_SM["min2"]:
            if retail_price_percentage_diff < 0:
                (
                    new_sm_margin,
                    new_factory_price,
                    new_bubr_margin,
                ) = calculate_bubr_margin(
                    current_factory_price,
                    current_sm_margin,
                    new_dealer_price,
                    taxes,
                )
            else:
                (
                    new_sm_margin,
                    new_factory_price,
                    new_bubr_margin,
                ) = calculate_all_prices_and_margins_changes(
                    base_cost_price,
                    current_factory_price,
                    new_dealer_price,
                    retail_price_percentage_diff,
                    taxes,
                )

        elif current_bubr_margin < MINIMUM_MARGINS_BUBR["min2"]:
            if retail_price_percentage_diff < 0:
                (
                    new_sm_margin,
                    new_factory_price,
                    new_bubr_margin,
                ) = calculate_all_prices_and_margins_changes(
                    base_cost_price,
                    current_factory_price,
                    new_dealer_price,
                    retail_price_percentage_diff,
                    taxes,
                )

            else:
                (
                    new_sm_margin,
                    new_factory_price,
                    new_bubr_margin,
                ) = calculate_bubr_margin(
                    current_factory_price,
                    current_sm_margin,
                    new_dealer_price,
                    taxes,
                )
        else:
            print("erro no cenário de calculo")
    else:
        (new_sm_margin, new_factory_price, new_bubr_margin) = calculate_bubr_margin(
            current_factory_price,
            current_sm_margin,
            new_dealer_price,
            taxes,
        )

    if suggested_retail_price == current_retail_price:
        dealer_net_price = current_dealer_net_price
    else:
        dealer_net_price = new_dealer_net_price

    # Calculating the Breakeven Point
    profit_scania = (
        sm_base_profit_real_calculated + bu_gross_profit_real_calculated
    ) * demand_next_12_months_retail_price
    profit_scania_best_price = (
        dealer_net_price - base_cost_price
    ) * demand_next_12_months_best_price

    if suggested_retail_price == current_retail_price:
        breakevenpoint = 0
    elif profit_scania != 0:
        if profit_scania_best_price > profit_scania:
            breakevenpoint = (
                abs(((profit_scania_best_price - profit_scania) / profit_scania)) * 100
            )
        else:
            breakevenpoint = (
                (profit_scania_best_price - profit_scania) / profit_scania
            ) * 100
    else:
        breakevenpoint = 0

    data = {
        "part_number": part_number,
        "suggested_factory_price": [new_factory_price],
        "suggested_dealer_net_price": [new_dealer_net_price],
        "suggested_dealer_price": [new_dealer_price],
        "breakeven_point": [breakevenpoint],
        "suggested_bubr_margin": [new_bubr_margin],
        "suggested_dealer_margin": [new_dealer_margin],
        "suggested_sm_margin": [new_sm_margin],
    }
    response = pd.DataFrame(data)

    return response


def main(conn_id):
    connection = connect_to_db(conn_id=conn_id)

    max_table_date = get_last_month_year_from_db(connection, "margin_and_price_calculations")
    """
    Extract
    """
    df_price_list_and_time_series = extract_from_price_list_and_time_series(connection, max_table_date)
    df_model_prediction_results = extract_from_model_prediction(connection, max_table_date)

    """ 
    Transform
        All the transformation needed to construct the "df_final" to insert it into [model_prediction_part_results]
    """
    df_calculated = pd.DataFrame()
    response = pd.DataFrame()

    # Rename the column from: "insertion_date", to: "month_year"
    df_price_list_and_time_series_renamed = df_price_list_and_time_series.rename(
        columns={"insertion_date": "month_year"}
    )

    # Making sure that the columns 'month_year' are in correct format
    # It is needed to make the merge
    df_price_list_and_time_series_renamed["month_year"] = pd.to_datetime(
        df_price_list_and_time_series_renamed["month_year"], format="%Y-%m-%d"
    )
    df_model_prediction_results["month_year"] = pd.to_datetime(
        df_model_prediction_results["month_year"], format="%Y-%m-%d"
    )

    # Merging both dfs: "df_price_list_and_time_series_renamed, df_model_prediction_results"
    # To run for all part_numbers, change 'inner' to 'left' and uncoment line 52 and 122
    df_merge = df_price_list_and_time_series_renamed.merge(
        df_model_prediction_results, on=["part_number", "month_year"], how="inner"
    )
    df_merge = df_merge.drop_duplicates(["part_number", "month_year"])

    df_merge = df_merge.rename(
        columns={
            "demand_best_price": "demand_next_12_months_best_price",
            "demand_retail_price": "demand_next_12_months_retail_price",
        }
    )

    part_number = [x for x in df_merge["part_number"]]
    suggested_retail_price = [x for x in df_merge["suggested_retail_price"]]
    base_cost_price = [x for x in df_merge["base_cost_price"]]
    current_factory_price = [x for x in df_merge["current_factory_price"]]
    current_retail_price = [x for x in df_merge["current_retail_price"]]
    current_sm_margin = [x for x in df_merge["current_sm_margin"]]
    current_bubr_margin = [x for x in df_merge["current_bubr_margin"]]
    current_dealer_margin = [x for x in df_merge["current_dealer_margin"]]
    ipi_percentage = [x for x in df_merge["ipi_percentage"]]
    icms_percentage = [x for x in df_merge["icms_percentage"]]
    pis_percentage = [x for x in df_merge["pis_percentage"]]
    cofins_percentage = [x for x in df_merge["cofins_percentage"]]
    demand_next_12_months_retail_price = [
        x for x in df_merge["demand_next_12_months_retail_price"]
    ]
    sm_base_profit_real = [x for x in df_merge["sm_base_profit_real_calculated"]]
    bu_gross_profit_real = [x for x in df_merge["bu_gross_profit_real_calculated"]]
    demand_next_12_months_best_price = [
        x for x in df_merge["demand_next_12_months_best_price"]
    ]
    current_dealer_net_price = [x for x in df_merge["current_dealer_net_price"]]
    try:
        response = [
            mount_calculate_from_retail_price_response(
                a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q
            )
            for a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q in zip(
                part_number,
                suggested_retail_price,
                base_cost_price,
                current_factory_price,
                current_retail_price,
                current_sm_margin,
                current_bubr_margin,
                current_dealer_margin,
                ipi_percentage,
                icms_percentage,
                pis_percentage,
                cofins_percentage,
                demand_next_12_months_retail_price,
                sm_base_profit_real,
                bu_gross_profit_real,
                demand_next_12_months_best_price,
                current_dealer_net_price,
            )
        ]
        response_array = np.array(response)
        response_new = list(
            np.reshape(response_array, (len(response_array), 8))
        )  # 8 because it's the number of columns

        # Append the result line
        df_calculated = pd.DataFrame(
            response_new,
            columns=[
                "part_number",
                "suggested_factory_price",
                "suggested_dealer_net_price",
                "suggested_dealer_price",
                "breakeven_point",
                "suggested_bubr_margin",
                "suggested_dealer_margin",
                "suggested_sm_margin",
            ],
        )
    except:
        pass

    # Rename all the columns from the df to match the columns from db
    df_final = pd.merge(df_merge, df_calculated, on=["part_number"], how="inner")

    # Rename all the columns from the df to match the columns from db
    df_final = df_final.rename(
        columns={
            "month_year": "insertion_date",
            "demand": "demand_last_month",
            "revenue": "revenue_last_month",
            "revenue_best_price": "revenue_next_12_months_best_price",
            "revenue_retail_price": "revenue_next_12_months_retail_price",
        }
    )

    # the demand can't be null, and making sure that is int
    df_final["demand_next_12_months_best_price"] = df_final[
        "demand_next_12_months_best_price"
    ].fillna(0)
    df_final["demand_next_12_months_best_price"] = df_final[
        "demand_next_12_months_best_price"
    ].astype(int)

    df_final["demand_next_12_months_retail_price"] = df_final[
        "demand_next_12_months_retail_price"
    ].fillna(0)
    df_final["demand_next_12_months_retail_price"] = df_final[
        "demand_next_12_months_retail_price"
    ].astype(int)

    df_final["demand_last_12_months"] = df_final["demand_last_12_months"].fillna(0)
    df_final["demand_last_12_months"] = df_final["demand_last_12_months"].astype(int)

    df_final["demand_last_month"] = df_final["demand_last_month"].fillna(0)
    df_final["demand_last_month"] = df_final["demand_last_month"].astype(int)

    """
    Loading all the "df_final" into [scania_parts_tab]
    """
    df_final = df_final[
        [
            "part_number",
            "suggested_factory_price",
            "suggested_dealer_net_price",
            "suggested_dealer_price",
            "suggested_retail_price",
            "suggested_sm_margin",
            "suggested_bubr_margin",
            "suggested_dealer_margin",
            "breakeven_point",
            "demand_next_12_months_best_price",
            "demand_next_12_months_retail_price",
            "revenue_next_12_months_best_price",
            "revenue_next_12_months_retail_price",
            "insertion_date",
        ]
    ]

    df_price_list_and_time_series["insertion_date"] = df_price_list_and_time_series[
        "insertion_date"
    ].apply(lambda x: dt.strftime(x, "%Y-%m-%d"))
    df_price_list_and_time_series["insertion_date"] = df_price_list_and_time_series[
        "insertion_date"
    ].apply(lambda x: dt.strptime(x, "%Y-%m-%d"))
    df_final_merge_pl = df_final.merge(
        df_price_list_and_time_series, on=["part_number", "insertion_date"], how="inner"
    )

    df_final_merge_pl_copy = df_final_merge_pl.copy()
    for i in range(len(df_final_merge_pl)):
        if df_final_merge_pl["breakeven_point"][i] < 0:
            df_final_merge_pl_copy["suggested_factory_price"][i] = df_final_merge_pl[
                "current_factory_price"
            ][i]

            df_final_merge_pl_copy["suggested_dealer_net_price"][i] = df_final_merge_pl[
                "current_dealer_net_price"
            ][i]

            df_final_merge_pl_copy["suggested_dealer_price"][i] = df_final_merge_pl[
                "current_dealer_price"
            ][i]

            df_final_merge_pl_copy["suggested_retail_price"][i] = df_final_merge_pl[
                "current_retail_price"
            ][i]

            df_final_merge_pl_copy["suggested_sm_margin"][i] = df_final_merge_pl[
                "current_sm_margin"
            ][i]

            df_final_merge_pl_copy["suggested_bubr_margin"][i] = df_final_merge_pl[
                "current_bubr_margin"
            ][i]

            df_final_merge_pl_copy["suggested_dealer_margin"][i] = df_final_merge_pl[
                "current_dealer_margin"
            ][i]

            df_final_merge_pl_copy["breakeven_point"][i] = 0

            df_final_merge_pl_copy["demand_next_12_months_best_price"][
                i
            ] = df_final_merge_pl["demand_next_12_months_retail_price"][i]

            df_final_merge_pl_copy["revenue_next_12_months_best_price"][
                i
            ] = df_final_merge_pl["revenue_next_12_months_retail_price"][i]

    df_final_refactored = df_final_merge_pl_copy[df_final.columns]
    print(f">> Processed DataFrame size: {len(df_final_refactored)}")


    try:
        print(">> Saving data to Database")
        load_to_db(
            connection,
            df=df_final_refactored,
            table="public.margin_and_price_calculations",
        )
        print(">> Database updated")

    except Exception as error:
        print(f"Error > {error}")

    finally:
        connection.close()


if __name__ == "__main__":
    main()
