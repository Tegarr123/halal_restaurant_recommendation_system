import pandas as pd

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from time import sleep
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import pandas as pd
from bs4 import BeautifulSoup
import time
import os
import warnings
import re
import pickle
from pathlib import Path
import json 
import logging
from selenium.webdriver.chrome.options import Options
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
warnings.filterwarnings("ignore")

def get_link(map_link):
    return map_link.split("/")[-1]

def wait_page(driver):
    WebDriverWait(driver, 10).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )

def get_place_attributes(driver, restaurant_menu_image):
    logging.info("Switch to tab Tentang / About")
    driver.find_element(By.XPATH, f"//button[contains(@aria-label, 'Tentang') or contains(@aria-label, 'About')]").click()
    # WebDriverWait(driver, 15).until(
    #     EC.presence_of_element_located(
    #         (By.XPATH, "//h1[contains(@class, 'fontHeadlineLarge')]")
    #     )
    # )

    # tabs = WebDriverWait(driver, 15).until(
    #     EC.presence_of_all_elements_located(
    #         (By.XPATH, "//button[@role='tab']")
    #     )
    # )

    # for tab in tabs:
    #     try:
    #         label = tab.get_attribute("aria-label")
    #         if label and "Bakmi GM" in label and ("Tentang" in label or "About" in label):
    #             driver.execute_script("arguments[0].click();", tab)
    #             break
    #     except:
    #         pass
    wait_page(driver)
    # driver.find_element(By.XPATH, "//button[@role='tab'][.//div[text()='Tentang' or text()='About']]").click()
    sleep(2)
    attributes = driver.find_elements(By.XPATH, "//li[@class='hpLkke']")
    attributes_text = []
    for attr in attributes:
        span_1 = attr.find_element(By.TAG_NAME, "div").find_elements(By.TAG_NAME, "span")[0]
        if "SwaGS" not in span_1.get_attribute("class"): continue
        span_2 = attr.find_element(By.TAG_NAME, "div").find_elements(By.TAG_NAME, "span")[1]
        attr_line = {"attribute_name":span_2.text}
        attributes_text.append(attr_line)
    logging.info(f"Found {len(attributes_text)} attributes")
    return attributes_text

def get_menu_image(driver):
    driver.find_element(By.XPATH, "//button[contains(@role, 'tab') and @aria-label='Menu']").click()
    logging.info(f"Swith to Menu Tab")
    wait_page(driver)
    sleep(2)
    menu_images = driver.find_elements(By.XPATH, "//div[contains(@class, 'ofKBgf') and contains(@class, 'J0JPTd')]")
    menu_image_lines = []
    for idx, menu_img in enumerate(menu_images):
        try:
            menu_name = menu_img.find_element(By.XPATH, ".//div[@class='KoY8Lc']/span[1]").text
            logging.info(f"Found menu name : {menu_name}")
        except NoSuchElementException:
            logging.warning("Image with no name detected")
            menu_name = "[NOT SPECIFIED]"
        img_link = menu_img.find_element(By.XPATH, 
                                             ".//button[@class='K4UgGe']/img[@class='DaSXdd' and contains(@src, 'https://lh3.googleusercontent.com')]").get_attribute("src")
        image_dict = {
            "menu_name":menu_name,
            "image_link":img_link
        }
        menu_image_lines.append(image_dict)
    logging.info(f"Found Image Total : {len(menu_image_lines)}")
    return menu_image_lines

def get_reviews_data(driver):
    logging.info(f"Clicking Ulasan/Reviews Tab")
    driver.find_element(By.XPATH, 
                        "//button[contains(@role, 'tab') and (contains(@aria-label,'Ulasan') or contains(@aria-label,'Reviews'))]").click()
    
    wait_page(driver)
    sleep(1)
    logging.info("Getting Review Tags")
    review_tags = driver.find_elements(By.XPATH, "//button[@class='e2moi' and @role='radio']")
    tags = []
    for tag in review_tags:
        tag_name = tag.find_element(By.XPATH, ".//div[1]/span[contains(@class, 'uEubGf')]").text 
        if tag_name != 'Semua' and tag_name != 'All':
            tags.append({"tag_name":tag_name})
    logging.info(f"Found Tags Total : {len(tags)}")
    
    logging.info(f"Sorting reviews to Newest")
    driver.find_element(By.XPATH,
                        "//button[@data-value='Urutkan' or @data-value='Sort']").click()
    
    sleep(1)
    driver.find_element(By.CSS_SELECTOR,
                        "div.fxNQSd[data-index='1']").click()
    sleep(2)
    reviews_container = WebDriverWait(driver, 10).until(EC.presence_of_element_located((
        By.XPATH, "//div[contains(@class, 'm6QErb') and contains(@class, 'DxyBCb')]"
    )))
    last_height = driver.execute_script("return arguments[0].scrollHeight", reviews_container)
    
    logging.info("Scrolling Reviews")
    while (len(driver.find_elements(By.XPATH, "//span[@class='wiI7pd']")) < 100):
        driver.execute_script("arguments[0].scrollTo(0, arguments[0].scrollHeight);", reviews_container)
        sleep(2)
        new_height = driver.execute_script("return arguments[0].scrollHeight", reviews_container)
        if new_height == last_height:
            print("Reached end of reviews.")
            break
        last_height = new_height
        sleep(1)
    
    logging.info("Finding Reviews that have comment text and whose star greater than 3")
    reviews = list(filter(lambda r : int(r.find_element(By.XPATH, ".//span[@class='kvMYJc' and @role='img']").get_attribute('aria-label').split(" ")[0]) > 3, 
                          driver.find_elements(By.XPATH, "//div[@class='GHT2ce' and .//span[@class='wiI7pd']]")))
    
    logging.info(f"Reviews Total = {len(reviews)}")
    reviews_data = []
    for review in reviews:
        try:
            review.find_element(By.XPATH, ".//button[text()='More' or text()='Lainnya']").click()
        except: pass
        sleep(0.1)
        # star = int(review.find_element(By.XPATH, ".//span[@class='kvMYJc' and @role='img']").get_attribute('aria-label').split(" ")[0])
        review_text = review.find_element(By.XPATH, ".//span[@class='wiI7pd']").text 
        # data = {
        #     "rating":star,
        #     "review_text":review_text
        # }
        reviews_data.append(review_text)
    return tags, reviews_data

def append_jsonlines(data:dict, path:Path):
    with open(path, 'a', encoding='utf-8') as f:
        json.dump(data, f)
        f.write("\n")
def main(row, doc_path):
    map_link = row.google_maps_link
    id = get_link(map_link)
    document_path = Path(os.path.join(doc_path, f"{id}.txt"))
    # menu_selected = list(map(lambda x: int(x), row.image_indexes.split(',')))
    
    logging.info(f"Getting Data From Link : {map_link}")
    # document_path = Path(f"notebooks/dataset/FromGoogleMaps/reviews_documents2/{id}.txt")
    if document_path.exists():
        logging.warning(f"Link with ID {id} already exists")
        return
    
    jsonl_path = Path("dataset/restaurants.jsonl")
    options = Options()
    # logging.warning("Executing With Headless Argument")
    # options.add_argument("--headless")
    options.set_preference("dom.webdriver.enabled", False)
    options.set_preference("useAutomationExtension", False)
    options.set_preference("intl.accept_languages", "id-ID")
    options.set_preference("media.peerconnection.enabled", False)

    options.add_argument("--width=1400")
    options.add_argument("--height=900")

    driver = webdriver.Firefox(
        service=Service(),
        options=options
    )
    
    driver.get(map_link)
    wait_page(driver)
    # while True:
    #     pass
    
    sleep(1)
    name = driver.find_element(By.XPATH, "//div[@class='lMbq3e']/div[1]/h1[1]").text
    logging.info(f"Found Place Name : {name}")
    address = driver.find_element(By.XPATH, "//div[contains(@class, 'rogA2c')]").text
    logging.info(f"Found Address : {address}")
    googleMapsUrl=map_link
    latitude=float(driver.current_url.replace("https://", "").split("/")[4][1:].split(",")[0])
    longitude=float(driver.current_url.replace("https://", "").split("/")[4][1:].split(",")[1])
    logging.info(f"Found (Latitude, Longitude) = {latitude, longitude}")
    image=driver.find_element(By.XPATH, "//img[contains(@src, 'https://lh3.googleusercontent.com')]").get_attribute("src")
    try:
        contact=driver.find_element(By.XPATH, "//button[@data-tooltip='Copy phone number' or @data-tooltip='Salin nomor telepon']/div[1]/div[2]/div[1]").text
        logging.info(f"Owner Contact : {contact}")
    except NoSuchElementException:
        logging.error(f"Cannot Found Owner's Contact, Set default to None")
        contact=None
    isVerified=False
    
    logging.info("Getting place attributes")
    WebDriverWait(driver, 15).until(
    EC.presence_of_all_elements_located(
            (By.XPATH, "//button[@role='tab']")
        )
    )
    attributes = get_place_attributes(driver, name)
    
    logging.info("Getting Image Data")
    menu_images = get_menu_image(driver)
    logging.info("Getting Review Texts and Tags")
    review_tags, reviews_text = get_reviews_data(driver)
    
    with open(document_path, 'w') as f:
        logging.info("Join all Review Texts into One Document")
        f.write("\n\n\n".join(reviews_text))
    
    jsonl = {
        "id":id,
        "name":name,
        "address":address,
        "googleMapsUrl":googleMapsUrl,
        "latitude":latitude,
        "longitude":longitude,
        "image":image,
        "contact":contact,
        "isVerified":isVerified,
        "place_attributes":attributes,
        "menu_images":menu_images,
        "review_tags":review_tags,
        "review_text":f"{id}.txt"
    }
    logging.info(f"Saved jsonl data into {jsonl_path}")
    append_jsonlines(jsonl, jsonl_path)
    driver.quit()
    pass


if __name__ == '__main__':
    
    url = "https://docs.google.com/spreadsheets/d/10juH2C6OD3Z0iZNlG9GcnI04gHnyw8dZKeQvbN6lbfo/export?gid=0&format=csv"
    restaurant_urls = pd.read_csv(url) 
    doc_path = "dataset/review_documents"
    
    for row in restaurant_urls.itertuples():
        main(row, doc_path)
    
    restaurant_count = 0
    jsonl_path = Path("dataset/restaurants.jsonl")
    
    with open(jsonl_path, 'r') as f:
        logging.info(f"Restaurant Data Collected : {len(list(f))}")
    
    
    
        
    