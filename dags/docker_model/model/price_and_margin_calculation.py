MINIMUM_MARGINS_SM = {"min1": 0.375, "min2": 0.153}
MINIMUM_MARGINS_BUBR = {"min1": 0.24, "min2": 0.05}
PIS_NON_BIPHASIC = 0.023
BIPHASIC_TAX = 0.0925


def margin_price(
    current_dealer_margin: float,
    current_sm_margin: float,
    current_bubr_margin: float,
    base_cost_price: float,
    pis_percentage: float,
    icms_percentage: float,
    cofins_percentage: float,
    ipi_percentage: float,
    tipo_margin: str,
):
    if tipo_margin == "min":
        margin_sm = 0.375
        margin_bu = 0.24
    elif tipo_margin == "rule":
        margin_sm = current_sm_margin * (1 - 0.1)
        margin_bu = current_bubr_margin * (1 - 0.1)

    margin_dealer = current_dealer_margin
    new_factory_price = base_cost_price / (1 - margin_sm)
    pis_tax = pis_percentage
    icms_tax = icms_percentage
    cofins_tax = cofins_percentage
    ipi_tax = ipi_percentage

    new_dealer_price = new_factory_price / (1 - margin_bu)
    new_dealer_price_with_taxes = new_dealer_price / (
        1 - (icms_tax + pis_tax + cofins_tax)
    )

    if pis_tax == 0.023:
        new_retail_price = (new_dealer_price_with_taxes * (1 + ipi_tax - icms_tax)) / (
            1 - margin_dealer - icms_tax
        )
    else:
        new_retail_price = (
            new_dealer_price_with_taxes * (1 + ipi_tax - icms_tax - 0.0925)
        ) / (1 - margin_dealer - icms_tax - 0.0925)

    return new_retail_price


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
    new_retail_price: float,
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
):
    taxes = {
        "ipi": ipi_percentage,
        "icms": icms_percentage,
        "pis": pis_percentage,
        "cofins": cofins_percentage,
    }

    pis_tax = pis_percentage
    icms_tax = icms_percentage
    ipi_tax = ipi_percentage
    cofins_tax = cofins_percentage

    user_group = "sm"

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
        (new_sm_margin, new_factory_price, new_bubr_margin) = calculate_bubr_margin(
            current_factory_price,
            current_sm_margin,
            new_dealer_price,
            taxes,
        )

    response = {
        "new_factory_price": new_factory_price,
        "new_dealer_net_price": new_dealer_net_price,
        "new_dealer_price": new_dealer_price,
        "new_retail_price": new_retail_price,
        "new_bubr_margin": new_bubr_margin,
        "new_dealer_margin": new_dealer_margin,
    }

    if user_group == "sm":
        response["new_sm_margin"] = new_sm_margin

    margin = (
        (new_factory_price - base_cost_price)
        + (new_dealer_net_price - new_factory_price)
        + (new_retail_price - new_dealer_price)
    )

    return margin


def func_new_dealer_net(
    new_retail_price: float,
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
):
    taxes = {
        "ipi": ipi_percentage,
        "icms": icms_percentage,
        "pis": pis_percentage,
        "cofins": cofins_percentage,
    }

    pis_tax = pis_percentage
    icms_tax = icms_percentage
    ipi_tax = ipi_percentage
    cofins_tax = cofins_percentage

    user_group = "sm"

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
        (new_sm_margin, new_factory_price, new_bubr_margin) = calculate_bubr_margin(
            current_factory_price,
            current_sm_margin,
            new_dealer_price,
            taxes,
        )

    response = {
        "new_factory_price": new_factory_price,
        "new_dealer_net_price": new_dealer_net_price,
        "new_dealer_price": new_dealer_price,
        "new_retail_price": new_retail_price,
        "new_bubr_margin": new_bubr_margin,
        "new_dealer_margin": new_dealer_margin,
    }

    if user_group == "sm":
        response["new_sm_margin"] = new_sm_margin

    return new_dealer_net_price


def func_new_bubr_margin(
    new_retail_price: float,
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
):
    taxes = {
        "ipi": ipi_percentage,
        "icms": icms_percentage,
        "pis": pis_percentage,
        "cofins": cofins_percentage,
    }

    pis_tax = pis_percentage
    icms_tax = icms_percentage
    ipi_tax = ipi_percentage
    cofins_tax = cofins_percentage

    user_group = "sm"

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
        (new_sm_margin, new_factory_price, new_bubr_margin) = calculate_bubr_margin(
            current_factory_price,
            current_sm_margin,
            new_dealer_price,
            taxes,
        )

    response = {
        "new_factory_price": new_factory_price,
        "new_dealer_net_price": new_dealer_net_price,
        "new_dealer_price": new_dealer_price,
        "new_retail_price": new_retail_price,
        "new_bubr_margin": new_bubr_margin,
        "new_dealer_margin": new_dealer_margin,
    }

    if user_group == "sm":
        response["new_sm_margin"] = new_sm_margin

    return new_bubr_margin


def func_new_sm_margin(
    new_retail_price: float,
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
):
    taxes = {
        "ipi": ipi_percentage,
        "icms": icms_percentage,
        "pis": pis_percentage,
        "cofins": cofins_percentage,
    }

    pis_tax = pis_percentage
    icms_tax = icms_percentage
    ipi_tax = ipi_percentage
    cofins_tax = cofins_percentage

    user_group = "sm"

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
        (new_sm_margin, new_factory_price, new_bubr_margin) = calculate_bubr_margin(
            current_factory_price,
            current_sm_margin,
            new_dealer_price,
            taxes,
        )

    response = {
        "new_factory_price": new_factory_price,
        "new_dealer_net_price": new_dealer_net_price,
        "new_dealer_price": new_dealer_price,
        "new_retail_price": new_retail_price,
        "new_bubr_margin": new_bubr_margin,
        "new_dealer_margin": new_dealer_margin,
    }

    if user_group == "sm":
        response["new_sm_margin"] = new_sm_margin

    return new_sm_margin
