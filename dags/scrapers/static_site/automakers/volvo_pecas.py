from scrapers.static_site.static_store_scraper import StaticStore
from scrapers.config.logger import LOGGER
import re


class VolvoPecas(StaticStore):
    """
    VolvoPecas is a child class built from the abstract class (StaticStore)

    ...

    Args:
        StaticStore (abc.ABCMeta): abstract class that implements static sites
    """

    def get_product_name(self, store_product):
        """
        Search the product name in the product HTML

        Args:
            store_product (bs4.element.Tag): BeautifulSoup object containing information about a product (HTML)

        Returns:
            product_name (str): Product's name
        """
        try:
            product_name = store_product.find("h3", class_="product-name").text.strip()

        except Exception as error:
            LOGGER.debug("get_product_name: " + str(error))

        return product_name

    def get_product_link(self, store_product):
        """
        Search the product link in the product HTML

        Args:
            store_product (bs4.element.Tag): BeautifulSoup object containing information about a product (HTML)

        Returns:
            product_link (str): Link to product page
        """
        try:
            product_link = store_product.find("a", class_="product-image")["href"]

        except Exception as error:
            LOGGER.debug("get_product_link: " + str(error))

        return product_link

    def get_product_image_link(self, store_product):
        """
        Search the product image link in the product HTML

        Args:
            store_product (bs4.element.Tag): BeautifulSoup object containing information about a product (HTML)

        Returns:
            product_image_link (str): Link to product image
        """
        try:
            product_image_link = store_product.find("a", class_="product-image").img[
                "src"
            ]

        except Exception as error:
            LOGGER.debug("get_product_image_link: " + str(error))

        return product_image_link

    def get_product_price(self, store_product):
        """
        Search the product price in the product HTML

        Args:
            store_product (bs4.element.Tag): BeautifulSoup object containing information about a product (HTML)

        Returns:
            product_price (str): Price of the product
        """
        try:
            product_price = re.sub(
                r"Por: ",
                "",
                store_product.find("span", class_="best-price").text.strip(),
            )

            product_price = product_price.replace(".", "")
            product_price = product_price.replace("R$", "")
            product_price = product_price.replace(",", ".")

        except Exception as error:
            LOGGER.debug("get_product_price: " + str(error))

        return product_price

    def get_product_code(self, store_product, product_name, product_link):
        """
        Search the product code (part number) in the product HTML

        Args:
            store_product (bs4.element.Tag): BeautifulSoup object containing information about a product (HTML)
            product_name (str): Product's name (not used in this store)
            product_link (str): Link to product page (not used in this store)

        Returns:
            product_code (str): Product code (Part number)
        """
        product_code = ""

        try:
            code = re.sub(
                r"Código: ",
                "",
                store_product.find("p", class_="codigo-produto").text.strip(),
            )

            if code.isdigit():
                product_code = code

        except Exception as error:
            LOGGER.debug("get_product_code: " + str(error))

        return product_code

    def get_product_info(self, product_link):
        product_info = dict()

        return product_info

    def get_product_details(self, product_link):
        """
        Search the product page for extra information

        Args:
            product_link (str): Link to product page

        Returns:
            product_details (str): Extra information / product description
        """
        product_details = ""

        try:
            self.product_site = self.get_html(product_link)

            application = self.product_site.find(
                "td", class_="value-field Aplicacao"
            ).text
            product_details = "aplicacao : " + self.strip_accents(application)

        except Exception as error:
            LOGGER.debug("get_product_details: " + str(error))

        return product_details

    def get_products(self, site):
        """
        Search all products on the site's search page

        Args:
            site (bs4.element.Tag): BeautifulSoup object containing the site's search page information (HTML)

        Returns:
            products (list): List containing the parts of the HTML referring to each product on the site's search page
        """
        try:
            products = site.findAll(
                "li", class_="pecas-e-acessorios-para-caminhoes-|-volvo-pecas"
            )

        except Exception as error:
            LOGGER.debug("get_products: " + str(error))

        return products
