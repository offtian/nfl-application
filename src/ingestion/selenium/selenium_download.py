import os
import logging
import traceback
from datetime import date
from typing import Optional, Dict, Any, List

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains

from src.ingestion.selenium.selenium_utils import (
    transform_url,
    check_exists_by_xpath,
    convert_table_to_csv,
    write_csv_data_to_filepath,
)
from src.ingestion.utils.custom_constants import (
    options,
    chrome_options,
    capabilities,
)

logger = logging.getLogger()

def download_nfl_wrapper(
    driver: webdriver,
    query_target: str = "games",
    file_dir: str=None,
    query_year_range: List[int]=[2022],
    query_team_name: Optional[str] = None,
    url_config: Dict[str, Any] = {"url": "www.draftkings.com"},
) -> None:
    try: 
        for yr in query_year_range:
            target_file_name = (
                f"{query_target}_{yr}.csv"
            )
            download_nfl(
                driver=driver,
                query_target=query_target,
                save_file_name=os.path.join(file_dir, target_file_name),
                query_year=yr,
                query_team_name=query_team_name,
                url_config=url_config
            )
    except Exception as e:
        logging.error(f"Failed to download {query_target} from {url_config['url']}")

def download_nfl(
    driver: webdriver,
    query_target: str = "games",
    save_file_name: str = None,
    query_year: Optional[int] = None,
    query_team_name: Optional[str] = None,
    url_config: Dict[str, Any] = {"url": "www.draftkings.com"},
) -> None:
    """Start query and scrape html table in csv format with selenium driver

    Args:
        driver selenium web driver
        query_target (str): target of query
        save_folder_path (str): file path where data is downloaded
        description_id (str): unique description of downloaded file
        query_year (int): year of interest for query
        query_team_name (str): team name of interest for query
        url_config (Dict[str, Any]): base config for the query
    """

    query_date = date.today().strftime("%Y%m%d")
    logging.info(f"Running the scrapper on {query_date}")

    logging.info

    url = transform_url(url_config["url"], query_year, query_team_name)
    driver.get(url)  # selenium driver

    # Check for cookie request, accept the cookie request if prompted
    # Check for cookie request, accept the cookie request if prompted
    if check_exists_by_xpath(driver, '//*[@class="qc-cmp2-consent-info"]'):
        wait(driver, 10).until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    "//span[text()='AGREE']",
                )
            )
        ).click()

    # Check if the table can hide partial rows (specifically for Team data)
    if check_exists_by_xpath(driver, "//*[@id='teams_active_toggle_partial_table']"):
        driver.execute_script(
            'document.querySelector("#teams_active_toggle_partial_table").click()'
        )

    try:
        # Check if the final table exists
        if check_exists_by_xpath(driver, f"""//*[@id='{url_config["table_id"]}']"""):
            logging.info(f"Found table with xpath {url_config['table_id']}")
            csv_data = convert_table_to_csv(driver, url_config["table_id"])
            logging.info(f"Transformed table to csv format. Ready for scraping.")

            write_csv_data_to_filepath(save_file_name, csv_data)
            logging.info(
                f"Successfully scraped {query_target} data to {save_file_name}"
            )
    except Exception as e:
        logging.error(f"Error: {e}")
        logging.error(f"Error: {traceback.format_exc()}")
        raise RuntimeError("Download failed")
    finally:
        driver.quit()
