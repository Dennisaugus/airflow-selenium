from scrapers.dynamic_site.dynamic_store_scraper import DynamicStore
from scrapers.config.logger import LOGGER


class Brasparts(DynamicStore):
    """
    Brasparts is a child class built from the abstract class (DynamicStore)

    ...

    Args:
        DynamicStore (abc.ABCMeta): abstract class that implements dynamic sites
    """

    def get_product_name(self, store_product):
        """
        Select an Element (product name) with a CSS Selector

        Args:
            store_product (requests_html.Element): Element containing product information

        Returns:
            product_name (str): Product's name
        """
        try:
            product_name = store_product.xpath(
                '//*[@class="product-name"]', first=True
            ).text
            """
            Problem: The page was encoded with the UTF-8 pattern, however in the request header the passed decoding pattern is ISO-8859-1, resulting in problems decoding letters with accents
            Solution: So we do the reverse process, encoding the string with the ISO-8859-1 pattern and then decoding it again with the UTF-8 pattern 
            """
            product_name = product_name.encode("iso-8859-1").decode("utf-8")
            product_name = product_name.replace(",", "").strip()

        except Exception as error:
            LOGGER.debug("get_product_name: " + str(error))

        return product_name

    def get_product_link(self, store_product):
        """
        Select an Element (product link) with a CSS Selector

        Args:
            store_product (requests_html.Element): Element containing product information

        Returns:
            product_link (str): Link to product page
        """
        try:
            product_link = store_product.xpath(
                '//*[@class="product-name"]', first=True
            ).attrs["href"]

        except Exception as error:
            LOGGER.debug("get_product_link: " + str(error))

        return product_link

    def get_product_image_link(self, store_product):
        """
        Select an Element (product image link) with a CSS Selector

        Args:
            store_product (requests_html.Element): Element containing product information

        Returns:
            product_image_link (str): Link to product image
        """
        try:
            product_image_link = store_product.xpath(
                '//*[@class="box-image"]/img', first=True
            ).attrs["src"]

        except Exception as error:
            LOGGER.debug("get_product_image_link: " + str(error))

        return product_image_link

    def get_product_price(self, store_product):
        """
        Select an Element (product price) with a CSS Selector

        Args:
            store_product (requests_html.Element): Element containing product information

        Returns:
            product_price (str): Price of the product
        """
        try:
            product_price = store_product.xpath(
                '//*[@class="price-promotion"]', first=True
            ).text

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
            store_product (requests_html.Element): Element containing product information
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

    def get_product_info(self, product_link, store_product):
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

        try:
            if not self.product_site:
                self.product_site = self.get_html(product_link)
            product_details = self.product_site.html.xpath(
                '//*[@class="board"]', first=True
            ).text
            product_details = product_details.encode("iso-8859-1").decode("utf-8")
            product_details = self.strip_accents(product_details)

        except Exception as error:
            LOGGER.debug("get_product_details: " + str(error))
            product_details = ""

        return product_details

    def get_products(self, site):
        """
        Search all products on the site's search page

        Args:
            site (requests_html.HTMLResponse): Text rendered by JavaScript

        Returns:
            products (list): List containing the parts of the HTML referring to each product on the site's search page
        """
        try:
            products = site.html.xpath('//*[@class="item flex"]')

        except Exception as error:
            LOGGER.debug("get_products: " + str(error))

        return products
