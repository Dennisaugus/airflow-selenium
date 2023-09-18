from dataclasses import dataclass, field

# Lists containing the lengths that the products codes of each brand can have
POSSIBLE_LENGTH_SCANIA = [5, 6, 7]
POSSIBLE_LENGTH_VOLVO = [5, 6, 7, 8]
POSSIBLE_LENGTH_IVECO = [7, 8, 9, 10]
POSSIBLE_LENGTH_VOLKSWAGEN = [9, 10, 11, 12]

# Dictionary containing the file_name of all stores
FILE_NAME_DICT = {
    "zanotto": "_zanotto_pecas_products",
    "gigante": "_gigante_autopecas_products",
    "rodoponta": "_rodoponta_products",
    "ja_cotei": "_ja_cotei_products",
    "scania_mercado_livre": "_scania_mercado_livre_products",
    "volvo_mercado_livre": "_volvo_mercado_livre_products",
    "volvo": "_volvo_pecas_products",
    "iveco_mercado_livre": "_iveco_mercado_livre_products",
    "volkswagen_mercado_livre": "_volkswagen_mercado_livre_products",
    "mega_diesel": "_mega_diesel_products",
    "mundo_caminhao": "_mundo_caminhao_products",
    "loja_dr3": "_loja_dr3_products",
    "vargas_parts": "_vargas_parts_products",
    "tresp": "_tresp_autopecas_products",
    "connect_parts": "_connect_parts_products",
    "brasparts": "_brasparts_products",
    "loja_stemac": "_loja_stemac_products",
}


@dataclass
class Settings:
    """
    Class for storing the data needed to instantiate the store classes

    ...

    Attributes:
    ----------
    STORES_DICT : dict
        dictionary with data to instantiate each of the stores
    """

    STORES_DICT: dict = field(
        default_factory=lambda: {
            "zanotto": {
                "page": 1,  # 93 pages
                "id_store": 1,
                "file_name": FILE_NAME_DICT["zanotto"],
                "url_to_format": "https://zanottopecas.com.br/page/{Page}/?s=scania&search_posttype=product",
            },
            "gigante": {
                "page": 1,  # 101 pages
                "id_store": 2,
                "possible_length": POSSIBLE_LENGTH_SCANIA,
                "file_name": FILE_NAME_DICT["gigante"],
                "url_to_format": "https://www.giganteautopeca.com.br/buscar?q=scania&pagina={Page}#",
            },
            "rodoponta": {
                "page": 1,  # 37 pages
                "id_store": 3,
                "possible_length": POSSIBLE_LENGTH_SCANIA,
                "file_name": FILE_NAME_DICT["rodoponta"],
                "url_to_format": "https://www.rodoponta.com.br/busca/scania////pag{Page}",
            },
            "ja_cotei": {
                "page": 1,  # 312 pages
                "id_store": 4,
                "possible_length": POSSIBLE_LENGTH_SCANIA,
                "file_name": FILE_NAME_DICT["ja_cotei"],
                "url_to_format": "https://www.jacotei.com.br/busca/?texto=scania&m=&n=32&p={Page}",
            },
            "scania_mercado_livre": {
                "page": 1,  # 1873 pages
                "step": 50,
                "id_store": 5,
                "possible_length": POSSIBLE_LENGTH_SCANIA,
                "file_name": FILE_NAME_DICT["scania_mercado_livre"],
                "url_to_format": "https://lista.mercadolivre.com.br/acessorios-veiculos/pe%C3%A7as-caminh%C3%B5es-scania_Desde_{Page}_NoIndex_True",
            },
            "volvo_mercado_livre": {
                "page": 1,  # 1009 pages
                "step": 50,
                "id_store": 7,
                "possible_length": POSSIBLE_LENGTH_VOLVO,
                "file_name": FILE_NAME_DICT["volvo_mercado_livre"],
                "url_to_format": "https://lista.mercadolivre.com.br/acessorios-veiculos/pe%C3%A7as-caminh%C3%B5es-volvo_Desde_{Page}_NoIndex_True",
            },
            "volvo": {
                "page": 1,  # 50 pages
                "id_store": 8,
                "file_name": FILE_NAME_DICT["volvo"],
                "url_to_format": "https://www.volvopecas.com.br/buscapagina?fq=C%3a%2f1%2f&PS=15&sl=19ccd66b-b568-43cb-a106-b52f9796f5cd&cc=3&sm=0&PageNumber={Page}",
            },
            "iveco_mercado_livre": {
                "page": 1,  # 1201 page
                "step": 50,
                "id_store": 9,
                "possible_length": POSSIBLE_LENGTH_IVECO,
                "file_name": FILE_NAME_DICT["iveco_mercado_livre"],
                "url_to_format": "https://lista.mercadolivre.com.br/acessorios-veiculos/pe%C3%A7as-caminh%C3%B5es-iveco_Desde_{Page}_NoIndex_True",
            },
            "volkswagen_mercado_livre": {
                "page": 1,  # 1969 pages
                "step": 50,
                "id_store": 10,
                "possible_length": POSSIBLE_LENGTH_VOLKSWAGEN,
                "file_name": FILE_NAME_DICT["volkswagen_mercado_livre"],
                "url_to_format": "https://lista.mercadolivre.com.br/acessorios-veiculos/pe%C3%A7as-caminh%C3%B5es-volkswagen_Desde_{Page}_NoIndex_True",
            },
            "mega_diesel": {
                "page": 1,  # 2 pages
                "id_store": 12,
                "possible_length": POSSIBLE_LENGTH_SCANIA,
                "file_name": FILE_NAME_DICT["mega_diesel"],
                "url_to_format": "https://www.megadieselpecas.com.br/buscar?q=scania&pagina={Page}",
            },
            "mundo_caminhao": {
                "page": 1,  # 1 page
                "id_store": 13,
                "possible_length": POSSIBLE_LENGTH_SCANIA,
                "file_name": FILE_NAME_DICT["mundo_caminhao"],
                "url_to_format": "https://www.mundodocaminhao.com.br/produtos?q=scania&page={Page}",
            },
            "loja_dr3": {
                "page": 1,  # 42 pages
                "id_store": 14,
                "possible_length": POSSIBLE_LENGTH_SCANIA,
                "file_name": FILE_NAME_DICT["loja_dr3"],
                "url_to_format": "https://www.lojadr3.com.br/procura/pagina-{Page}?cat=&procura=scania#conteudo",
            },
            "vargas_parts": {
                "page": 1,  # 2 pages
                "id_store": 15,
                "possible_length": POSSIBLE_LENGTH_SCANIA,
                "file_name": FILE_NAME_DICT["vargas_parts"],
                "url_to_format": "https://vargas.parts/categoria-produto/scania/page/{Page}/",
            },
            "tresp": {
                "page": 1,  # 177 pages
                "id_store": 6,
                "scrolldown_count": 3,
                "possible_length": POSSIBLE_LENGTH_SCANIA,
                "file_name": FILE_NAME_DICT["tresp"],
                "url_to_format": "https://www.3pautopecas.com.br/loja/busca.php?loja=970537&palavra_busca=scania&pg={Page}",
            },
            "connect_parts": {
                "page": 1,  # 1 page
                "id_store": 16,
                "scrolldown_count": 20,
                "possible_length": POSSIBLE_LENGTH_SCANIA,
                "file_name": FILE_NAME_DICT["connect_parts"],
                "url_to_format": "https://www.connectparts.com.br/busca?q=scania&Page={Page}",
            },
            "brasparts": {
                "page": 1,  # 6 pages
                "id_store": 18,
                "scrolldown_count": 20,
                "possible_length": POSSIBLE_LENGTH_SCANIA,
                "file_name": FILE_NAME_DICT["brasparts"],
                "url_to_format": "https://www.brasparts.com.br/loja/busca.php?loja=699301&palavra_busca=scania&pg={Page}",
            },
            "loja_stemac": {
                "page": 1,  # 1 page
                "id_store": 17,
                "scrolldown_count": 10,
                "possible_length": POSSIBLE_LENGTH_SCANIA,
                "file_name": FILE_NAME_DICT["loja_stemac"],
                "url_to_format": "https://loja.stemac.com.br/loja/busca.php?loja=692753&palavra_busca=scania&pg={Page}",
            },
        }
    )
