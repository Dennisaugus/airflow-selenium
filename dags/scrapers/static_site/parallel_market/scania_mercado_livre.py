from .mercado_livre import MercadoLivre
from scrapers.config.logger import LOGGER


class ScaniaMercadoLivre(MercadoLivre):
    """
    ScaniaMercadoLivre is a child class built from the abstract class (MercadoLivre)

    ...

    Args:
        MercadoLivre (abc.ABCMeta): abstract class that implements stores present in Mercado Livre
    """

    def get_product_code(self, store_product, product_name, product_link):
        """
        Search the product code (part number) in the product name or on the product page

        Args:
            store_product (bs4.element.Tag): BeautifulSoup object containing information about a product (HTML)
            product_name (str): Product's name
            product_link (str): Link to product page

        Returns:
            product_code (str): Product code (Part number)
        """
        product_code = ""
        possible_codes = []

        # Looking for code in product name
        for word in product_name.split():
            if word.isdigit():
                possible_codes.append(word)

        if len(possible_codes) > 0:
            for code in possible_codes:
                if len(code) in self.possible_length:
                    product_code = code

        # Looking for code on product page
        if not product_code:
            if not self.product_site:
                self.product_site = self.get_html(product_link)

            try:
                tags_span = self.product_site.findAll(
                    "span", class_="andes-table__column--value"
                )

                for tag in tags_span:
                    code = tag.text
                    if code.isdigit() and len(code) in self.possible_length:
                        product_code = code

            except Exception as error:
                LOGGER.debug("get_product_code: " + str(error))

        return product_code
