from scrapers.static_site.static_store_scraper import StaticStore
from scrapers.config.logger import LOGGER
import os


class GiganteAutopecas(StaticStore):
    """
    GiganteAutopecas is a child class built from the abstract class (StaticStore)

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
                "a", class_="nome-produto cor-secundaria"
            ).text
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
                "a", class_="nome-produto cor-secundaria"
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
            product_image_link = store_product.find("img", class_="imagem-principal")[
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
            try:
                product_price = store_product.find(
                    "strong", class_="preco-promocional cor-principal"
                ).text

            except:
                product_price = store_product.find(
                    "strong", class_="preco-promocional cor-principal titulo"
                ).text

            product_price = product_price.replace(".", "")
            product_price = product_price.replace("R$ ", "").strip()
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

        #     description_text = self.product_site.find(
        #         "div", class_="tab-pane active"
        #     ).text
        #     description_lines = description_text.replace("\r", "").split("\n")

        #     for line in description_lines:
        #         line = line.lower().replace("\xa0", "")
        #         line = self.strip_accents(line)

        #         try:
        #             if "codigo" in line:
        #                 info_title, info = line.split(":")

        #                 info_list = list()
        #                 for code in info.replace(" ", "").split("/"):
        #                     if code.isdigit():
        #                         info_list.append(code)

        #                 if info_list:
        #                     product_info[info_title] = info_list

        #         except:
        #             for i in line.split("codigo"):
        #                 info_list = list()

        #                 for word in i.replace(":", " ").split():
        #                     if word.isdigit() and len(word) > 4:
        #                         info_list.append(word)

        #                 if info_list and i.find("original") != -1:
        #                     product_info["codigo original"] = info_list

        #                 elif info_list and i.find("fabrica") != -1:
        #                     product_info["codigo de fabrica"] = info_list

        #                 elif info_list and i.find(":") != -1:
        #                     product_info["codigo"] = info_list

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

        #     description = self.product_site.find("div", class_="tab-pane active").text
        #     description = os.linesep.join([s for s in description.splitlines() if s])
        #     description = description.split("A Gigante Auto Peças")

        #     product_details = description[0].replace("\xa0", "").lower().strip()
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
            products = site.findAll("li", class_="span3")

        except Exception as error:
            LOGGER.debug("get_products: " + str(error))

        return products
