from os import getenv
import time
from ..infra.logger import Logger
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


logger = Logger.get_logger(__name__)

class B3Scraper:
    def __init__(self):
        self.url = getenv("BS3_SITE_URL")

    def extract_data(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--window-size=1920,1080")

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        driver.get(self.url)

        wait = WebDriverWait(driver, 10)
        data = []
        page = 1

        while True:
            logger.info(f"Accessing page {page}...")
            wait.until(EC.presence_of_all_elements_located((By.TAG_NAME, "tbody")))
            rows = driver.find_elements(By.CSS_SELECTOR, "tbody tr")

            for row in rows:
                columns = row.find_elements(By.TAG_NAME, "td")
                values = [col.text.strip() for col in columns]
                data.append(values)

            try:
                next_button = driver.find_element(By.CLASS_NAME, "pagination-next")
                if "disabled" in next_button.get_attribute("class"):
                    logger.info("Last page reached.")
                    break
                next_button.click()
                page += 1
                time.sleep(2)
            except Exception as e:
                logger.warning(f"'Next' button not found or error: {e}")
                break

        driver.quit()
        logger.info(f"Extraction finished. Total rows collected: {len(data)}")
        return data