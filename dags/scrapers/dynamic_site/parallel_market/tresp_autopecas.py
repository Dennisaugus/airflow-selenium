from scrapers.dynamic_site.dynamic_store_scraper import DynamicStore
from scrapers.config.logger import LOGGER
from urllib.parse import unquote


class TrespAutopecas(DynamicStore):
    """
    TrespAutopecas is a child class built from the abstract class (DynamicStore)

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
            product_name = store_product.xpath('//*[@class="product__name"]', first=True).text
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
                '//*[@class="product__link"]', first=True
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
                '//*[@class="product__image"]', first=True
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
            product_price = (
                store_product.xpath('//*[@class="prices__price"]', first=True).text
            )

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

        try:
            if not self.product_site:
                self.product_site = self.get_html(product_link)

            tags_li = self.product_site.html.xpath('//span[@itemprop="description"]//li')

            for tag in tags_li:
                tag = tag.text.lower()

                if "números similares" in tag:
                    info_title, info = tag.split(":")
                    info_list = info.split()
                    info_title = unquote(info_title)
                    info_title = self.strip_accents(info_title)

                    product_info[info_title] = info_list

                elif "marca" in tag:
                    info_title, info = tag.split(":")
                    info = info.replace("\xa0", "").strip()

                    product_info[info_title] = info

        except Exception as error:
            LOGGER.debug("get_product_info: " + str(error))

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

        if not self.product_site:
            self.product_site = self.get_html(product_link)

        try:
            tags_li = self.product_site.html.xpath('//span[@itemprop="description"]//li')

            for tag in tags_li:
                tag = tag.text.lower()
                tag = unquote(tag)

                if "modelo" in tag:
                    info_title, info = tag.split(":")
                    info_title = self.strip_accents(info_title)

                    product_details = (
                        product_details + " - " + info_title + " : " + info
                    )

        except Exception as error:
            LOGGER.debug("get_product_details: " + str(error))

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
            products = site.html.xpath('//*[@class="showcase__item"]')

        except Exception as error:
            LOGGER.debug("get_products: " + str(error))

        return products
