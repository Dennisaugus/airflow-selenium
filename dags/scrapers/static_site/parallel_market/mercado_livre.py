from scrapers.static_site.static_store_scraper import StaticStore
from scrapers.config.logger import LOGGER
from abc import abstractmethod


class MercadoLivre(StaticStore):
    """
    MercadoLivre is a child class built from the abstract class (StaticStore). MercadoLivre also works like a blueprint for other classes 

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
                "h2", class_="ui-search-item__title shops__item-title"
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
            product_link = store_product.find("a", class_="ui-search-link")["href"]

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
        
        product_image_link = ''
        
        # try:
        #     product_image_link = store_product.find(
        #         "a", class_="ui-search-link"
        #     ).img['src']

        #     print(product_image_link)

        # except Exception as error:
        #     LOGGER.debug("get_product_image_link: " + str(error))

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
            price_tag = store_product.find("span", class_="andes-money-amount ui-search-price__part shops__price-part andes-money-amount--cents-superscript")

            price_fraction = price_tag.find(
                "span", class_="andes-money-amount__fraction"
            ).text.strip()

            price_fraction = price_fraction.replace(".", "")

            try:
                cents = price_tag.find("span", class_="andes-money-amount__cents andes-money-amount__cents--superscript-24").text.strip()

                product_price = price_fraction + "." + cents

            except:
                product_price = price_fraction
            
        except Exception as error:
            LOGGER.debug("get_product_price: " + str(error))

        return product_price

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

        #     description = self.product_site.find(
        #         "p", class_="ui-pdp-description__content"
        #     )

        #     product_details = (
        #         str(description)
        #         .replace('<p class="ui-pdp-description__content">', "")
        #         .replace("<br/>", " ")
        #         .replace("</p>", "")
        #         .replace("*", "")
        #         .replace("•", "")
        #         .replace("»", "")
        #         .strip()
        #     )

        #     product_details = self.strip_accents(product_details).lower()

        # except Exception as error:
        #     LOGGER.debug("get_product_details: " + str(error))

        return product_details

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

        #     tags_li = self.product_site.findAll("li", class_="ui-pdp-list__item")

        #     for tag in tags_li:
        #         tag = tag.text

        #         if "OEM" in tag:
        #             info_title, info = tag.replace(" ", "").split(":")
        #             product_info[info_title] = info

        # except Exception as error:
        #     LOGGER.debug("get_product_info: " + str(error))

        return product_info

    def get_products(self, site):
        """
        Search all products on the site's search page

        Args:
            site (bs4.element.Tag): BeautifulSoup object containing the site's search page information (HTML)

        Returns:
            products (list): List containing the parts of the HTML referring to each product on the site's search page
        """
        try:
            products = site.findAll("div", class_="ui-search-result__wrapper shops__result-wrapper")

        except Exception as error:
            LOGGER.debug("get_products: " + str(error))

        return products

    @abstractmethod
    def get_product_code(self, store_product, product_name, product_link):
        pass
