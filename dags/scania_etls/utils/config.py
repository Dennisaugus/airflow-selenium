"""Configuration dataclasses"""
from dataclasses import dataclass, field


@dataclass
class SalesRede:
    """Settings for sales_rede_att table preprocessing"""

    KEEP_COLUMNS: list = field(
        default_factory=lambda: [
            "DATA_FECHAMENTO",
            "CD_PECAS",
            "LINHA",
            "QT_VENDIDA",
            "DEALER_ID",
            "MODELO_CHASSI",
            "ANO_FABRICACAO",
            "DS_TYPE_CUSTOMER",
            "FATURAMENTO",
            "VL_NET_BILLING",
            "IMPOSTOS",
            "ODOMETRO",
            "CTG_VENDA",
            "CD_SERVICE_ORDER",
            "TIPO_OS",
            "AGRUPAMENTO",
            "DS_CITY",
            "AC_FEDERATIVE_UNIT",
            "NU_IDENTITY_PRODUCT",
            "VL_NF_CANCELED",
            "DS_STATUS_SERVICE_ORDER",
            "DS_SALE_SOURCE",
            "NUM_CHASSI",
        ]
    )

    COLUMN_NAMES: list = field(
        default_factory=lambda: [
            "sale_date",
            "part_number",
            "part_line",
            "item_quantity",
            "id_dealer",
            "modelo_chassi",
            "ano_fabricacao",
            "ds_type_customer",
            "sale_revenue",
            "vl_net_billing",
            "impostos",
            "odometro",
            "ctg_venda",
            "cd_service_order",
            "os_type",
            "sales_group",
            "ds_city",
            "ac_federative_unit",
            "nu_identity_product",
            "vl_nf_canceled",
            "ds_status_service_order",
            "ds_sale_source",
            "num_chassi",
        ]
    )

    COLUMN_ORDER: list = field(
        default_factory=lambda: [
            "sale_date",
            "month_year",
            "part_number",
            "part_line",
            "os_type",
            "sales_group",
            "sale_revenue",
            "item_quantity",
            "id_dealer",
            "modelo_chassi",
            "ano_fabricacao",
            "ds_type_customer",
            "vl_net_billing",
            "impostos",
            "odometro",
            "ctg_venda",
            "cd_service_order",
            "ds_city",
            "ac_federative_unit",
            "nu_identity_product",
            "vl_nf_canceled",
            "ds_status_service_order",
            "ds_sale_source",
            "num_chassi",
        ]
    )

    NOT_NULL_COLS: list = field(
        default_factory=lambda: [
            "id_dealer",
            "part_number",
            "part_line",
            "item_quantity",
            "sale_revenue",
            "sale_date",
        ]
    )


@dataclass
class PriceListBU:
    """"""

    KEEP_COLUMNS: list = field(
        default_factory=lambda: [
            "ITEM",
            "DESCRICAO",
            "STATUS",
            "PRECO_PUBLICO",
            "PRG",
            "CC_DC",
            "FPRICE_PRATICADO",
            "COFINS",
            "PIS",
            "IPI",
            "PRECO_DEALER",
            "PRECO_BALCAO",
            "ICMS",
            "SERIE",
            "CLAS_FISCAL",
            "MC_PEÇA",
            "IMPOSTOS",
            "FP R$",
            "FP R$ IMPOSTO",
            "MARGEM",
            "R$ IPI",
            "PISCOFINS",
            "ICMS",
            "CUSTO",
            "MC R$",
            "MC %",
            "Dealer Net",
            "BALCÃO 30D",
            "Demanda 12 meses",
            "Gross Price R$",
            "Forecast",
            "Balcão",
            "Gross USD Scania",
            "Gross USD Oficial",
        ]
    )

    COLUMN_NAMES: list = field(
        default_factory=lambda: [
            "part_number",
            "part_description",
            "part_status",
            "public_price",
            "prg",
            "cc_dc",
            "fprice_praticado",
            "cofins",
            "pis",
            "ipi",
            "dealer_price",
            "shelf_price",
            "icms",
            "serie",
            "ncm",
            "part_contribution_margin",
            "taxes",
            "fprice_real",
            "fprice_real_tax",
            "margin",
            "ipi_real",
            "piscofins_real",
            "icms_real",
            "part_cost",
            "contribution_margin_real",
            "contribution_margin_pct",
            "dealer_net",
            "shelf_30d",
            "demand_12_months",
            "gross_price_real",
            "forecast",
            "shelf",
            "gross_dolar_scania",
            "gross_dolar_oficial",
        ]
    )

    MONETARY_COLUMNS_TO_CLEAN: list = field(
        default_factory=lambda: [
            "ipi_real",
            "piscofins_real",
            "icms_real",
            "part_cost",
            "fprice_real",
            "fprice_real_tax",
            "contribution_margin_real",
            "dealer_net",
            "shelf_30d",
            "shelf",
            "gross_dolar_scania",
            "gross_dolar_oficial",
        ]
    )

    PCT_COLUMNS_TO_CLEAN: list = field(
        default_factory=lambda: ["margin", "contribution_margin_pct"]
    )


@dataclass
class PriceListSM:

    KEEP_COLUMNS: list = field(
        default_factory=lambda: [
            "Part number",
            "Part name",
            "CC/DC",
            "PRG",
            "Source\n(Origem)",
            "Use\n(Uso)",
            '"Status"',
            "Gross Price USD",
            "BD",
            "MD",
            "DT",
            "FP source",
            "12 last months\nQty",
            "Base Cost*\nBRL",
            "Base Cost*\nUSD",
            "Factory price\nBRL",
            "Factory price\nUSD",
            "S&M base margin",
            "S&M base profit\nBRL",
            "S&M base profit\nUSD",
        ]
    )

    COLUMN_NAMES: list = field(
        default_factory=lambda: [
            "part_number",
            "part_description",
            "cc_dc",
            "prg",
            "origin",
            "uso",
            "part_status",
            "gross_price_usd",
            "bd",
            "md",
            "dt",
            "fp_source",
            "demand_last_12_months",
            "base_cost_real",
            "base_cost_usd",
            "factory_price_real",
            "factory_price_usd",
            "sm_base_margin",
            "sm_base_profit_real",
            "sm_base_profit_usd",
        ]
    )

    NOT_NULL_COLS: list = field(
        default_factory=lambda: [
            "part_number",
            "cc_dc",
            "prg",
            "origin",
            "gross_price_usd",
            "base_cost_real",
            "base_cost_usd",
            "sm_base_margin",
            "sm_base_profit_real",
            "sm_base_profit_usd",
        ]
    )
