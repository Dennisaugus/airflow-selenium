from scrapers.static_site.static_store_scraper import StaticStore
from scrapers.config.logger import LOGGER
import os


class Rodoponta(StaticStore):
    """
    Rodoponta is a child class built from the abstract class (StaticStore)

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
            product_name = store_product.find("div", class_="info").find("h2").text
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
            product_link = store_product.find("div", class_="thumb").find("a")["href"]

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
            product_image_link = store_product.find("div", class_="inset").img["src"]

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
                store_product.find("div", class_="price2").find("strong").text
            )

            product_price = product_price.replace(".", "")
            product_price = product_price.replace("R$ ", "").strip()
            product_price = product_price.replace(",", ".")

        except Exception as error:
            LOGGER.debug("get_product_price: " + str(error))

        return product_price

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
                tags_p = self.product_site.select("div.tab > p")

                for p in tags_p:
                    p = p.text.lower()

                    original_index = p.find("original:")

                    if original_index != -1:
                        code = (
                            p[original_index + len("original:") :]
                            .split("/")[-1]
                            .strip()
                        )
                        code_size = len(code)

                        if code.isdigit() and code_size in self.possible_length:
                            product_code = code

            except Exception as error:
                LOGGER.debug("get_product_code: " + str(error))

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

        # try:
        #     if not self.product_site:
        #         self.product_site = self.get_html(product_link)

        #     tags_p = self.product_site.select("div.tab > p")

        #     for tag in tags_p:
        #         tag = tag.text.lower().replace("\xa0", "")

        #         if "marca:" in tag:
        #             info_title, info = tag.split(":")
        #             info = info.strip()
        #             product_info[info_title] = info

        #         if "cód." in tag:
        #             separator = ":" if ":" in tag else "."
        #             info_title, info = tag.split(separator)
        #             info_title = info_title.replace("\u00f3", "o")
        #             info_list = info.replace(" ", "").split("/")

        #             product_info[info_title] = info_list

        # except Exception as error:
        #     LOGGER.debug("get_product_info: " + str(error))

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

        # try:
        #     if not self.product_site:
        #         self.product_site = self.get_html(product_link)

        #     for div in self.product_site.findAll("div", class_="tab"):
        #         product_details = product_details + " " + div.text.strip()

        #     product_details = (
        #         product_details.replace("\xa0", "").replace("Descrição", "").strip()
        #     )
        #     product_details = os.linesep.join(
        #         [s for s in product_details.splitlines() if s]
        #     )
        #     product_details = self.strip_accents(product_details)

        # except Exception as error:
        #     LOGGER.debug("get_product_details: " + str(error))

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
            products = site.findAll("div", class_="prod-item")

        except Exception as error:
            LOGGER.debug("get_products: " + str(error))

        return products
