import logging
import csv
import os
from typing import Optional, List, Any
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
)

logger = logging.getLogger()

ignored_exceptions = (
    NoSuchElementException,
    StaleElementReferenceException,
)


def check_exists_by_xpath(driver: webdriver, xpath: str) -> bool:
    """Helper function to check if an element exists by xpath

    Args:
        driver (webdriver): webdriver instance
        xpath (str): XPATH of an element

    Returns:
        bool: if the element exists
    """
    try:
        driver.find_element(By.XPATH, xpath)
    except NoSuchElementException:
        logger.info(f"Element {xpath} not found.")
        return False
    return True


def write_csv_data_to_filepath(filepath: str, data: List) -> None:
    """Write csv data to target filepath.

    Args:
        filepath (str): target filepath
        data (list): csv data to write to target filepath
    """
    with open(filepath, "w") as result_file:
        wr = csv.writer(result_file, dialect="excel")
        wr.writerows(data)
        logger.info(f"Data written to {filepath}.")


def transform_url(
    url: str, query_year: Optional[int] = None, team_name: Optional[str] = None
) -> str:
    """Helper function to standardise the url to enable dynamic queries

    Args:
        url (str): url pattern for target query
        query_year (Optional[int], optional): year of interest. Defaults to None.
        team_name (Optional[str], optional): team name of interest. Defaults to None.

    Returns:
        str: final url for query
    """
    # if query_year is None and team_name is None:
    #     url = url.replace("/{team_name}", "").replace("/{year}", "")
    # else:
    url = url.format(year=query_year, team_name=team_name)

    if not url.startswith("https://"):
        url = "https://" + url

    url = url.encode("ascii", "ignore").decode("unicode_escape")

    return url


def convert_table_to_csv(driver: webdriver, table_id: str) -> List[List[Any]]:
    # Convert table to csv file
    driver.execute_script(
        f"""
        document.querySelector("#{table_id}_sh > div > ul > li.hasmore > div > ul > li:nth-child(3) > button").click()
        """
    )

    csv_data = driver.find_element(
        by=By.XPATH, value="//*[contains(@id,'csv')]"
    ).text.splitlines()[4:]

    csv_data = [row.split(",") for row in csv_data]
    return csv_data
