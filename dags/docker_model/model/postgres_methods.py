import pandas as pd
import psycopg2
import os


def connect_to_db():
    # Psycopg2 database connection
    connection = psycopg2.connect(
        host=os.getenv("HOST"),
        user=os.getenv("DATABASE_USER"),
        password=os.getenv("DATABASE_PASSWORD"),
        dbname=os.getenv("DATABASE"),
        port=os.getenv("PORT"),
    )

    return connection


# Function to transform raw data in time series (All data)
def etl_timeseries_all(connection) -> pd.DataFrame:
    sql = "SELECT * FROM time_series_model"
    df = pd.read_sql(sql=sql, con=connection)

    return df


# Function to get data from current month (Test dataset - from price list)
def current_data_all(date: str, connection) -> pd.DataFrame:
    sql = f"""
        SELECT
            part_number,
            part_cost,
            month_year,
            current_retail_price as shelf_price,
            prg,
            cc,
            dc,
            base_cost_price,
            ipi_percentage/100 as ipi_percentage,
            icms_percentage/100 as icms_percentage,
            pis_percentage/100 as pis_percentage,
            cofins_percentage/100 as cofins_percentage,
            current_dealer_margin,
            current_sm_margin,
            current_bubr_margin,
            current_factory_price,
            current_dealer_price,
            sm_base_profit_real_calculated,
            bu_gross_profit_real_calculated
        FROM price_list
        WHERE month_year = '{date}'
    """
    df = pd.read_sql(sql=sql, con=connection)

    return df


def median_market_price(connection):
    sql = """
        WITH grouped AS (
            SELECT
                product_code,
                DATE_TRUNC('month', job_datetime)::DATE AS month_year,
                product_price,
                id_competitor
            FROM competitors_products cp
            GROUP BY product_code, month_year, product_price, id_competitor
            ORDER BY product_price
        ), median_calc AS (
            SELECT
                product_code,
                month_year,
                PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY product_price) AS market_price
            FROM grouped
            JOIN competitors USING (id_competitor)
            WHERE id_competitor_type <> 2
            AND id_competitor <> 5
            GROUP BY product_code, month_year
        )
        SELECT
            product_code,
            PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY market_price) AS median_market_price
        FROM median_calc
        GROUP BY product_code
    """
    df = pd.read_sql(sql=sql, con=connection)

    return df
