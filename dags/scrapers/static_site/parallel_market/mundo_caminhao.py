from scrapers.static_site.static_store_scraper import StaticStore
from scrapers.config.logger import LOGGER


class MundoCaminhao(StaticStore):
    """
    MundoCaminhao is a child class built from the abstract class (StaticStore)

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
            product_name = store_product.find("h2", class_="text").text
            product_name = product_name.replace(",", "")

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
            product_link = store_product.find(
                "a",
                class_="d-flex flex-column justify-content-between w-100 h-100 product-link position-relative",
            )["href"]

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
            product_image_link = store_product.find("img", class_="img-fluid")["src"]

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
            product_price = store_product.find("span", class_="price total").text

            product_price = product_price.replace(".", "")
            product_price = product_price.replace("R$", "")
            product_price = product_price.replace(",", ".")

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
        product_info = dict()

        return product_info

    def get_product_details(self, product_link):
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
                "div", class_="col-6 col-sm-6 col-md-4 col-lg-3 mb-4"
            )

        except Exception as error:
            LOGGER.debug("get_products: " + str(error))

        return products
