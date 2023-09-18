from datetime import datetime as dt
import pandas as pd
import numpy as np
import calendar

from ..utils.postgres_methods import load_to_db
from ..utils.db_connection import connect_to_db


def get_last_date_from_db(connection, table_name: str, column_name) -> str:
    """
    Searches the most recent date in the table passed by parameter
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT TO_CHAR(max({column_name}), 'YYYY-MM-DD') AS max_date
                FROM {table_name}
            """
            )
            max_date = cursor.fetchone()[0]

            max_table_date = dt.strptime(max_date, "%Y-%m-%d").date()

            print(f">> Latest date from '{table_name}' table: {max_table_date}")

        return max_table_date

    except Exception as error:
        print(f">> get_last_month_year_from_db ['{table_name}'] > {error}")

    finally:
        cursor.close()

def current_month_info():
    month_year = dt.today().replace(day=1).date()

    year = dt.today().year
    month = dt.today().month
    last_day_of_month = calendar.monthrange(year, month)[1]

    last_day_of_month_string = f"{year}-{month}-{last_day_of_month}"
    last_day_of_month_date = dt.strptime(last_day_of_month_string, "%Y-%m-%d").date()

    return month_year, last_day_of_month_date

def get_number_of_sale_dates(connection, month_year):
    """
    Searches the most recent date in the table passed by parameter
    """
    month_year_string = dt.strftime(month_year, "%Y-%m-%d")
    try:
        with connection.cursor() as cursor:
            cursor.execute(
            f"""
            SELECT COUNT(DISTINCT(sale_date))
            FROM raw_data.raw_sales_rede rsr
            WHERE month_year = '{month_year_string}'
            """
            )

            number_of_sale_date = cursor.fetchone()[0]

            print(f">> Number of sale_date in raw_data.raw_sales_rede table in {month_year_string}: {number_of_sale_date}")

        return number_of_sale_date

    except Exception as error:
        print(f">> get_number_of_sale_dates > {error}")

    finally:
        cursor.close()


def check_if_update(raw_price_list_bu_date,
                    raw_price_list_sm_date,
                    raw_sales_rede_date,
                    raw_fleet_date,
                    price_list_date,
                    month_year,
                    last_day_of_month_date,
                    connection):
    
    price_list_month_to_be_updatated_date = price_list_date + pd.DateOffset(months=1)

    if month_year == price_list_date:
        print('Database updated!')
        check_update = False

    elif month_year == price_list_month_to_be_updatated_date:
        print('Checking if data is ready to be updated...')

        if raw_price_list_bu_date == raw_price_list_sm_date == raw_fleet_date == month_year:
            print('Raw price lists (BU and S&M) and raw fleet are available')
        
            number_of_sale_date = get_number_of_sale_dates(connection, month_year)

            # Check to see if there is sale date in the end of the month.
            # We use -3 days of the last day of the month due to weekend.
            check_sale_date_criteria = last_day_of_month_date - pd.DateOffset(days=3)

            minimum_number_of_sale_date = 20 

            if (raw_sales_rede_date >= check_sale_date_criteria) and (number_of_sale_date >= minimum_number_of_sale_date):
                print('Raw sales rede data is available and met the criteria.')
                print('The data is ready to be updated!')
                check_update = True
            else:
                raise ValueError("Raw sales rede data is incomplete!")
        
        else:
            raise ValueError("Raw price lists or raw fleet is not updated yet!")
    
    else:
        check_update = False
        raise ValueError("Fail to get the date to analyze!")
    

    return check_update

def main(conn_id):
    connection = connect_to_db(conn_id=conn_id)

    latest_date_raw_price_list_bu = get_last_date_from_db(connection, "raw_data.raw_price_list_bu", "month_year")
    latest_date_raw_price_list_sm = get_last_date_from_db(connection, "raw_data.raw_price_list_sm", "month_year")
    latest_date_raw_sales_rede = get_last_date_from_db(connection, "raw_data.raw_sales_rede", "sale_date")
    latest_date_raw_fleet = get_last_date_from_db(connection, "raw_data.raw_fleet", "month_year")
    latest_date_price_list = get_last_date_from_db(connection, "public.price_list", "month_year")
    month_year, last_day_of_month_date = current_month_info()

    check_update = check_if_update(
        latest_date_raw_price_list_bu,
        latest_date_raw_price_list_sm,
        latest_date_raw_sales_rede,
        latest_date_raw_fleet,
        latest_date_price_list,
        month_year,
        last_day_of_month_date,
        connection
    )
    
    if check_update:
        print("Database is about to be updated!")
    else:
        print("Database is not ready to be updated!")
if __name__ == "__main__":
    main()