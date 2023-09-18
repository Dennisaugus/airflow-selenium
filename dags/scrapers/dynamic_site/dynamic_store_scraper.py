from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
# from selenium.webdriver.chrome.service import Service
from requests_html import HTMLSession
from abc import ABC, abstractmethod

from selenium import webdriver
from datetime import datetime
import pandas as pd
import unicodedata
import time
import json
import os

from scrapers.config.logger import LOGGER


class DynamicStore(ABC):
    """
    DynamicStore works like a blueprint for other classes (which implement dynamic sites). It allows the creation of a set of methods that must be created within any child classes built from the abstract class

    ...

    Args:
        ABC (abc.ABCMeta): decorates methods of the base class as abstract and then register concrete classes as implementations of the abstract base

    ...

    Attributes:
    ----------
    page : int
        search page number
    step : int
        step from one page to another, normally this step is equal to 1
    date : str
        search date
    id_store : int
        store id for identification in the database
    file_name : str
        contains the name of the store which will be added to the name of the CSV file
    product_site : str
        variable that will store the BeautifulSoup object containing the product page information
    url_to_format : str
        url to be formatted by adding the 'page' number
    possible_length : list()
        list of possible sizes that product_code can have
    returned_products : list()
        list where products with product_name, product_price and product_code are added
    """

    page: int
    step: int
    date: str
    id_store: int
    file_name: str
    product_site: str
    url_to_format: str
    scrolldown_count: int
    possible_length: list()
    returned_products: list()

    def __init__(
        self,
        id_store,
        page,
        file_name,
        url_to_format,
        scrolldown_count,
        step=1,
        possible_length=[],
    ):
        """
        Constructs all the necessary attributes for the store object.
        """
        self.page = page
        self.step = step
        self.product_site = ""
        self.id_store = id_store
        self.file_name = file_name
        self.returned_products = []
        # Initialize the GET requests
        self.session = HTMLSession()
        self.url_to_format = url_to_format
        self.possible_length = possible_length
        self.scrolldown_count = scrolldown_count
        self.date = datetime.today().strftime("%Y-%m-%d %H:%M:%S")

    def get_html(self, url):
        """
        Makes a GET request to the page of interest (url), gets the response and renders the dynamic content just as a web browser would.

        Args:
            url (str): Network address where the page of interest is located

        Returns:
            site (requests_html.HTMLResponse): Text rendered by JavaScript
        """
        try:
            # site = 0
            # Launch a headless browser with selenium
            # LOGGER.info("1")
            options = webdriver.ChromeOptions()
            # options.binary_location = "/opt/google/chrome/google-chrome"
            # options.add_argument("--no-sandbox")
            # options.add_argument("--headless")
            # options.add_argument("--disable-gpu")
            # options.add_argument("--disable-dev-shm-usage")
            # options.add_argument("--remote-debugging-port=9222")
            # options.add_argument("--disable-extensions")
            # options.add_argument("--disable-popup-blocking")
            # options.add_argument("--disable-default-apps")
            # options.add_argument("--disable-infobars")
            # options.add_argument("--disable-web-security")
            # options.add_argument("--disable-features=IsolateOrigins,site-per-process")
            # options.add_argument("--user-data-dir=/tmp/user-data")
            # options.add_argument("--window-size=1920,1080")
            # options.add_argument("--start-maximized")

            # LOGGER.info("1.5")

            # try:
            #     driver_service = Service()
            #     driver = webdriver.Chrome(service=driver_service, options=options)
            #     print(driver)
            # except Exception as error:
            #     LOGGER.error("webdriver: " + str(error))
            # try:
            #     chrome_driver_binary = "/usr/bin/chromedriver"
            #     driver = webdriver.Chrome(
            #         executable_path=chrome_driver_binary, chrome_options=options
            #     )
            #     # driver = webdriver.Chrome(
            #     #     executable_path="/opt/google/chrome/google-chrome", options=options
            #     # )
            # except Exception as error:
            #     LOGGER.error("webdriver: " + str(error))

            options = webdriver.ChromeOptions()
            options.add_argument("--headless")
            # driver = webdriver.Chrome(options=options)
            driver = webdriver.Chrome(
                service=ChromeService(ChromeDriverManager().install()), options=options
            )
            print(driver)
            LOGGER.info("2")

            # Navigate to the website and execute JavaScript
            driver.get(url)
            html = driver.page_source
            print(html)
            LOGGER.info("driver: ", driver)
            # driver.execute_script('document.getElementById("button").click();')
            LOGGER.info("3")

            # # Wait for the page to fully load
            # # driver.implicitly_wait(10)
            # LOGGER.info("4")

            # site = self.session.get(driver.current_url)
            # LOGGER.info("site:" + site.html)

            # # Quit the browser
            # driver.quit()
            # print('antes')
            site = self.session.get(url)
            # print('no meio')
            site.html.render(sleep=100, scrolldown=self.scrolldown_count)
            # print('depois')
            # print(site)

        except Exception as error:
            LOGGER.error("get_html: " + str(error))

        return site

    def save_products(self):
        """
        Saves the data present in the 'returned_products' list into a CSV file in the 'scrapers_data' folder
        """
        LOGGER.info(f"All Products: {len(self.returned_products)}")

        # Turns the list into a DataFrame
        df = pd.DataFrame(self.returned_products)
        df = df.drop_duplicates(subset=["product_code", "product_price"])

        # Add the date column (job_datetime)
        date_timestamp = pd.to_datetime(self.date)
        df.insert(0, "job_datetime", date_timestamp)

        # Handling date to set 'complete_file_name'
        date_split = self.date.split()
        complete_file_name = date_split[0] + self.file_name + ".csv"

        # Set 'data_folder' path
        data_folder = "./scrapers_data"
        if not os.path.exists(data_folder):
            os.mkdir(data_folder)

        full_file_name = os.path.join(data_folder, complete_file_name)

        # Check if CSV file already exists
        if os.path.isfile(full_file_name):
            # If so, the data is added to the same file
            df.to_csv(
                full_file_name,
                mode="a",
                header=False,
                index=False,
            )
        else:
            # If not, the file is created
            df.to_csv(full_file_name, index=False)

        LOGGER.info("Saved to CSV!")

    def format_url(self):
        """
        Format the 'url_to_format' by adding the 'page' number of interest

        Returns:
            url (str): Network address where the page of interest is located
        """
        url = self.url_to_format.format(Page=self.page)

        return url

    def format_product_code(self, product_code):
        """
        Fill in with zeros to the left so that the product code has 7 digits

        Args:
            product_code (str): Product code (Part number)

        Returns:
            formatted_code (str): Formatted product code
        """
        formatted_code = str(product_code).zfill(7)

        return formatted_code

    def strip_accents(self, string):
        """
        Remove the accents present in the 'string'

        Args:
            string (str): Text that possibly has accents

        Returns:
            string_without_accents (str): The same text present in 'string', but without accents
        """
        string_without_accents = "".join(
            c
            for c in unicodedata.normalize("NFD", string)
            if unicodedata.category(c) != "Mn"
        )

        return string_without_accents

    def next_page_index(self):
        """
        Calculates the index of the next page that will be searched

        Returns:
            next_page (int): The sum between the current page number and the step from one page to another
        """
        next_page = self.page + self.step

        return next_page

    def run(self):
        """
        Function in charge of calling the other functions and performing the search for all information
        """
        start = time.perf_counter()

        while True:
            if "{Page}" in self.url_to_format:
                url = self.format_url()
            else:
                url = self.url_to_format
            LOGGER.info("url:" + url)
            site = self.get_html(url)
            # LOGGER.info("site:" + site)

            # Receives the HTML parts referring to each product present on the search page
            store_products = self.get_products(site)

            # If no product was found the while breaks
            if not store_products:
                break

            print(f"Scraping page {self.page}")

            # Search the information for each product in the list
            for store_product in store_products:
                try:
                    product_name = self.strip_accents(
                        self.get_product_name(store_product)
                    )
                    product_link = self.get_product_link(store_product)
                    product_code = self.get_product_code(
                        store_product, product_name, product_link
                    )

                    # If no code is found the product is not added to the 'returned_products' list
                    if product_code:
                        print("Product code -> ", product_code)
                        product_image_link = self.get_product_image_link(store_product)
                        print("Image link -> ", product_image_link)
                        product_price = float(self.get_product_price(store_product))
                        print("Product price -> ", product_price)
                        product_info = json.dumps(
                            self.get_product_info(product_link, store_product), indent=4
                        )
                        product_details = self.get_product_details(product_link)
                        product_code = self.format_product_code(product_code)

                        print(product_code, product_price)
                        product = {
                            "product_code": product_code,
                            "id_competitor": self.id_store,
                            "product_name": product_name,
                            "product_details": product_details,
                            "product_price": product_price,
                            "product_link": product_link,
                            "product_image_link": product_image_link,
                            "product_info": product_info,
                        }
                        print(product)
                        print(product.keys())
                        print(product.values())

                        self.returned_products.append(product)

                except Exception as error:
                    LOGGER.debug("run: " + str(error))
                    continue

                # product_site is reset
                self.product_site = ""

            self.save_products()
            self.returned_products.clear()

            if self.page == -1:
                break
            self.page = self.next_page_index()

        fin = time.perf_counter() - start
        LOGGER.info("TIME:" + str(fin))

    @abstractmethod
    def get_product_name(self, store_product):
        pass

    @abstractmethod
    def get_product_link(self, store_product):
        pass

    @abstractmethod
    def get_product_image_link(self, store_product):
        pass

    @abstractmethod
    def get_product_price(self, store_product):
        pass

    @abstractmethod
    def get_product_code(self, store_product, product_name, product_link):
        pass

    @abstractmethod
    def get_product_info(self, product_link, store_product):
        pass

    @abstractmethod
    def get_product_details(self, product_link):
        pass

    @abstractmethod
    def get_products(self, site):
        pass
