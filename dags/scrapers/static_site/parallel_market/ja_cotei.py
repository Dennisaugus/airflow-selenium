from scrapers.static_site.static_store_scraper import StaticStore
from scrapers.config.logger import LOGGER
from urllib.parse import unquote


class JaCotei(StaticStore):
    """
    JaCotei is a child class built from the abstract class (StaticStore)

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
            product_name = store_product.find(
                "h3", class_="text-center tituloProdutosNovos"
            ).text
            product_name = product_name.replace(",", "").strip()

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
            intermediate_link = store_product.find(
                "h3", class_="text-center tituloProdutosNovos"
            ).find("a")["href"]

            if not intermediate_link.startswith("https"):
                product_link = "https://www.jacotei.com.br" + intermediate_link

            else:
                intermediate_site = self.get_html(intermediate_link)
                product_link = intermediate_site.find("a")["href"]
                product_link = unquote(product_link)

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
            product_image_link = store_product.find("div", class_="item active").img[
                "data-original"
            ]

            if not product_image_link.startswith("https"):
                product_image_link = "https:" + product_image_link

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
            product_price = (
                store_product.find("p", class_="menoPrecoDe text-center")
                .find("span")
                .text
            )

            product_price = product_price.replace(".", "")
            product_price = product_price.replace("R$", "")
            product_price = product_price.replace(",", ".").strip()

        except Exception as error:
            LOGGER.debug("get_product_price: " + str(error))

        return product_price

    def get_product_code(self, store_product, product_name, product_link):
        """
        Search the product code (part number) in the product name

        Args:
            store_product (bs4.element.Tag): BeautifulSoup object containing information about a product (HTML)
            product_name (str): Product's name
            product_link (str): Link to product page (not used in this store)

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

        return product_code

    def get_product_info(self, product_link):
        """
        Search the product page for information associated with a title

        Args:
            product_link (str): Link to product page

        Returns:
            product_info (dict): Title (key) + Information (value)
        """
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
                "article",
                class_="produtosS col-lg-4 col-md-4 col-sm-6 col-xs-12 produtos_vertical",
            )

        except Exception as error:
            LOGGER.debug("get_products: " + str(error))

        return products
