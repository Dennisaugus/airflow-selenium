from orbit.forecaster.full_bayes import FullBayesianForecaster
from scipy.optimize import minimize
from orbit.models import DLT
from scipy.stats import norm
import pandas as pd
import numpy as np


# Funções necessárias para o modelo
def dlt_model(df_timeseries: pd.DataFrame) -> FullBayesianForecaster:
    df = df_timeseries.copy()
    dlt_reg = DLT(
        response_col="demand",
        date_col="month_year",
        regressor_col=["sale_unit_price"],
        regressor_sign=["-"],
        seasonality=12,
        estimator="stan-mcmc",
        seed=8888,
        num_warmup=1000,
        num_sample=100,
        cores=1,
    )

    return dlt_reg.fit(df=df)


# Previsão de demanda 12 meses
def demand_forecast_year(
    price_model: FullBayesianForecaster, date: str, price: float, periods: int
) -> pd.DataFrame:
    df_data = pd.DataFrame(
        data={
            "month_year": pd.date_range(start=date, periods=periods, freq="MS"),
            "sale_unit_price": np.repeat(a=price, repeats=periods),
        }
    )
    df_forecast = price_model.predict(df=df_data, decompose=True, seed=8888)
    df_forecast = df_forecast[["month_year", "prediction"]].sum()
    df_forecast["prediction"] = np.where(
        df_forecast["prediction"] < 0, 0, df_forecast["prediction"]
    )

    return df_forecast["prediction"]


# Margem 12 meses
def margin_revenue_year(
    price: float,
    price_model: FullBayesianForecaster,
    demand_func,
    cost_func,
    date: str,
    periods: int,
    current_dealer_margin: float,
    current_retail_price: float,
    pis_percentage: float,
    icms_percentage: float,
    ipi_percentage: float,
    cofins_percentage: float,
    current_factory_price: float,
    current_sm_margin: float,
    current_bubr_margin: float,
    base_cost_price: float,
) -> float:
    margin = cost_func(
        new_retail_price=price,
        current_dealer_margin=current_dealer_margin,
        current_retail_price=current_retail_price,
        pis_percentage=pis_percentage,
        icms_percentage=icms_percentage,
        ipi_percentage=ipi_percentage,
        cofins_percentage=cofins_percentage,
        current_factory_price=current_factory_price,
        current_sm_margin=current_sm_margin,
        current_bubr_margin=current_bubr_margin,
        base_cost_price=base_cost_price,
    )

    demand = demand_func(
        price_model=price_model, date=date, price=price, periods=periods
    )
    m_revenue = demand * margin

    return -m_revenue


def best_price(
    part_retail_price: float,
    date: str,
    periods: int,
    margin_revenue_year_func,
    demand_func,
    price_model: FullBayesianForecaster,
    cost_func,
    current_dealer_margin: float,
    current_retail_price: float,
    pis_percentage: float,
    icms_percentage: float,
    ipi_percentage: float,
    cofins_percentage: float,
    current_factory_price: float,
    current_sm_margin: float,
    current_bubr_margin: float,
    base_cost_price: float,
    dc: int,
    median_market_price: float,
) -> pd.DataFrame:
    x0 = part_retail_price
    pct_change = 0.2
    max_price = x0 + x0 * pct_change
    min_price = x0 - x0 * pct_change
    bounds = [(min_price, max_price)]

    result = minimize(
        fun=margin_revenue_year_func,
        x0=x0,
        args=(
            price_model,
            demand_func,
            cost_func,
            date,
            periods,
            current_dealer_margin,
            current_retail_price,
            pis_percentage,
            icms_percentage,
            ipi_percentage,
            cofins_percentage,
            current_factory_price,
            current_sm_margin,
            current_bubr_margin,
            base_cost_price,
        ),
        method="SLSQP",
        bounds=bounds,
    )

    sug_price = result["x"][0]

    if np.isnan(median_market_price):
        if sug_price > part_retail_price:
            if dc in [1, 2, 5]:
                best_price = sug_price * 0.95  # antes estava com part_retail_price
            else:
                best_price = sug_price
        else:
            best_price = sug_price
    else:
        if median_market_price > part_retail_price:
            if sug_price > part_retail_price:
                best_price = sug_price
            else:
                best_price = sug_price * 1.05  # antes estava com part_retail_price
        else:
            if sug_price < part_retail_price:
                best_price = sug_price
            else:
                best_price = sug_price * 0.95  # antes estava com part_retail_price

    return best_price


def percent_bad(
    price_model: FullBayesianForecaster,
    date: str,
    price: float,
    demand_test: float,
    periods: int,
):
    df_data = pd.DataFrame(
        data={
            "month_year": pd.date_range(start=date, periods=periods, freq="MS"),
            "sale_unit_price": np.repeat(a=price, repeats=periods),
        }
    )
    model = price_model.predict(df=df_data, decompose=True, seed=8888)
    desv_pad = (model["prediction_95"].sum() - model["prediction"].sum()) / 1.645
    z = (demand_test - model["prediction"].sum()) / desv_pad

    return norm.cdf(z)
