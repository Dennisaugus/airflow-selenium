from scrapers.config.stores_info import Settings
from scrapers.config.logger import LOGGER
from .dynamic_site import TrespAutopecas, ConnectParts, Brasparts, LojaStemac
from .static_site import (
    JaCotei,
    Rodoponta,
    ZanottoPecas,
    GiganteAutopecas,
    ScaniaMercadoLivre,
    MegaDiesel,
    MundoCaminhao,
    LojaDr3,
    VargasParts,
    VolvoPecas,
    IvecoMercadoLivre,
    VolvoMercadoLivre,
    VolkswagenMercadoLivre,
)


# Load data from DataClass 'Settings'
settings = Settings()

# Call the method that searches the sites


def zanotto_run():
    zanotto_scraper = ZanottoPecas(**settings.STORES_DICT["zanotto"])
    try:
        zanotto_scraper.run()
    except Exception as error:
        LOGGER.error("run: " + str(error))


def gigante_autopecas_run():
    gigante_scraper = GiganteAutopecas(**settings.STORES_DICT["gigante"])
    try:
        gigante_scraper.run()
    except Exception as error:
        LOGGER.error("run: " + str(error))


def rodoponta_run():
    rodoponta_scraper = Rodoponta(**settings.STORES_DICT["rodoponta"])
    try:
        rodoponta_scraper.run()
    except Exception as error:
        LOGGER.error("run: " + str(error))


def ja_cotei_run():
    jacotei_scraper = JaCotei(**settings.STORES_DICT["ja_cotei"])
    try:
        jacotei_scraper.run()
    except Exception as error:
        LOGGER.error("run: " + str(error))


def scania_ml_run():
    scania_ml_scraper = ScaniaMercadoLivre(
        **settings.STORES_DICT["scania_mercado_livre"]
    )
    try:
        scania_ml_scraper.run()
    except Exception as error:
        LOGGER.error("run: " + str(error))


def mega_diesel_run():
    mega_diesel_scraper = MegaDiesel(**settings.STORES_DICT["mega_diesel"])
    try:
        mega_diesel_scraper.run()
    except Exception as error:
        LOGGER.error("run: " + str(error))


def mundo_caminhao_run():
    mundo_caminhao_scraper = MundoCaminhao(**settings.STORES_DICT["mundo_caminhao"])
    try:
        mundo_caminhao_scraper.run()
    except Exception as error:
        LOGGER.error("run: " + str(error))


def loja_dr3_run():
    loja_dr3_scraper = LojaDr3(**settings.STORES_DICT["loja_dr3"])
    try:
        loja_dr3_scraper.run()
    except Exception as error:
        LOGGER.error("run: " + str(error))


def vargas_parts_run():
    vargas_parts_scraper = VargasParts(**settings.STORES_DICT["vargas_parts"])
    try:
        vargas_parts_scraper.run()
    except Exception as error:
        LOGGER.error("run: " + str(error))


# Automakers
def volvo_pecas_run():
    volvo_pecas_scraper = VolvoPecas(**settings.STORES_DICT["volvo"])
    try:
        volvo_pecas_scraper.run()
    except Exception as error:
        LOGGER.error("run: " + str(error))


def volvo_ml_run():
    volvo_ml_scraper = VolvoMercadoLivre(**settings.STORES_DICT["volvo_mercado_livre"])
    try:
        volvo_ml_scraper.run()
    except Exception as error:
        LOGGER.error("run: " + str(error))


def iveco_ml_run():
    iveco_ml_scraper = IvecoMercadoLivre(**settings.STORES_DICT["iveco_mercado_livre"])
    try:
        iveco_ml_scraper.run()
    except Exception as error:
        LOGGER.error("run: " + str(error))


def volkswagwn_ml_run():
    volkswagen_ml_scraper = VolkswagenMercadoLivre(
        **settings.STORES_DICT["volkswagen_mercado_livre"]
    )
    try:
        volkswagen_ml_scraper.run()
    except Exception as error:
        LOGGER.error("run: " + str(error))


# Dynamic
def tresp_autopecas_run():
    tresp_scraper = TrespAutopecas(**settings.STORES_DICT["tresp"])
    try:
        tresp_scraper.run()
    except Exception as error:
        LOGGER.error("run: " + str(error))


def connect_parts_run():
    connect_parts_scraper = ConnectParts(**settings.STORES_DICT["connect_parts"])
    try:
        connect_parts_scraper.run()
    except Exception as error:
        LOGGER.error("run: " + str(error))


def brasparts_run():
    brasparts_scraper = Brasparts(**settings.STORES_DICT["brasparts"])
    try:
        brasparts_scraper.run()
    except Exception as error:
        LOGGER.error("run: " + str(error))


def loja_stemac_run():
    loja_stemac_scraper = LojaStemac(**settings.STORES_DICT["loja_stemac"])
    try:
        loja_stemac_scraper.run()
    except Exception as error:
        LOGGER.error("run: " + str(error))
