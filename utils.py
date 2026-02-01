from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium import webdriver
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import duckdb
from selenium.webdriver.support.ui import WebDriverWait
from time import sleep
from selenium.webdriver.support import expected_conditions as EC
import pandasql as pdsql
import shutil

RESTAURANT_LINKS_PATH = "https://docs.google.com/spreadsheets/d/10juH2C6OD3Z0iZNlG9GcnI04gHnyw8dZKeQvbN6lbfo/export?gid=0&format=csv"
SCRAPED_RESTAURANTS_PATH = "data/restaurants.duckdb"

def init_firefox_driver(headless=False):
    options = Options()
    if headless:
        options.add_argument("--headless")
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
    return driver


def get_restaurant_links():
    return pd.read_csv(RESTAURANT_LINKS_PATH).itertuples()

def extract_link(map_link):
    return map_link.split("/")[-1]

def get_restaurants_db():
    return duckdb.connect(database=SCRAPED_RESTAURANTS_PATH, read_only=False)

def get_restaurants_information(driver, row):
    print(f"Scraping information for restaurant: {row.restaurant_name}")
    def get_link(map_link):
        return map_link.split("/")[-1]
    try:
        contact=driver.find_element(By.XPATH, "//button[@data-tooltip='Copy phone number' or @data-tooltip='Salin nomor telepon']/div[1]/div[2]/div[1]").text
    except NoSuchElementException:
        contact=None
    return {
        "restaurant_id": get_link(str(row.google_maps_link)),
        "restaurant_name": str(row.restaurant_name),
        "restaurant_link": str(row.google_maps_link),
        "restaurant_halal_certificate": str(row.halal_certification_number),
        "restaurant_address": driver.find_element(By.XPATH, "//div[contains(@class, 'rogA2c')]/div[1]").text,
        "latitude": float(driver.current_url.replace("https://", "").split("/")[4][1:].split(",")[0]),
        "longitude": float(driver.current_url.replace("https://", "").split("/")[4][1:].split(",")[1]),
        "restaurant_contact": contact
    }

def is_restaurant_scraped(db_conn, restaurant_id):
    query = f"""
    SELECT COUNT(1) AS count
    FROM restaurant
    WHERE restaurant_id = ?
    """
    result = db_conn.execute(query, (restaurant_id,)).fetchone()
    return result[0] > 0

def insert_restaurant_information(db_conn, restaurant_info):
    new_restaurant = pd.DataFrame(restaurant_info, index=[0])
    query = """
        insert into restaurant select * from new_restaurant
    """
    
    db_conn.execute(query)

def open_restaurant_page(driver, google_maps_link):
    driver.get(google_maps_link)
    wait_page(driver)
    sleep(1)
    
def wait_page(driver):
    WebDriverWait(driver, 10).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    ) 

def scrape_all_reviews(driver):
    driver.find_element(By.XPATH, 
                        "//button[contains(@role, 'tab') and (contains(@aria-label,'Ulasan') or contains(@aria-label,'Reviews'))]").click()
    wait_page(driver)
    sleep(1)
    review_tags = driver.find_elements(By.XPATH, "//button[@class='e2moi' and @role='radio']")
    reviews = []
    for num, tag in enumerate(review_tags, start=1):
        
        tag.click()
        comment_tag = tag.get_attribute("aria-label").split(",")[0]
        if comment_tag == "Semua ulasan" or comment_tag == "All reviews":
            continue
        
        reviews_container = WebDriverWait(driver, 10).until(EC.presence_of_element_located((
            By.XPATH, "//div[contains(@class, 'm6QErb') and contains(@class, 'DxyBCb')]"
        )))
        last_height = driver.execute_script("return arguments[0].scrollHeight", reviews_container)
        while True:
            driver.execute_script("arguments[0].scrollTo(0, arguments[0].scrollHeight);", reviews_container)
            sleep(1)
            new_height = driver.execute_script("return arguments[0].scrollHeight", reviews_container)
            if new_height == last_height:
                print("Reached end of reviews.")
                break
            last_height = new_height
            sleep(1)
            
        review_elements = driver.find_elements(By.XPATH, "//div[@class='GHT2ce' and .//span[@class='wiI7pd']]")
        
        for review_num, review in enumerate(review_elements, start=1):
            print(f"TAG[{num}/{len(review_tags)}] REVIEW[{review_num}/{len(review_elements)}]", end="\r")
            # print(review)
            review_rating  = int(review.find_element(By.XPATH, ".//span[@class='kvMYJc' and @role='img']").get_attribute('aria-label').split(" ")[0])
            try:
                review.find_element(By.XPATH, ".//button[text()='More' or text()='Lainnya']").click()
            except: pass
            review_text = review.find_element(By.XPATH, ".//span[@class='wiI7pd']").text

            reviews.append({
                "review_rating": review_rating,
                "comment_tag": comment_tag,
                "comment_text": review_text
            })
    return reviews

def insert_reviews_information(db_conn, restaurant_id, reviews):
    print(f"Inserting {len(reviews)} reviews for restaurant_id={restaurant_id}")
    if len(reviews) == 0:
        return
    reviews_df = pd.DataFrame(reviews)
    reviews_df["restaurant_id"] = restaurant_id
    query = """
        insert into reviews select restaurant_id, review_rating, comment_tag, comment_text from reviews_df
    """
    db_conn.execute(query)



def save_dataset_to_parquet(db_conn=duckdb.connect('data/restaurants.duckdb')):
    backup_data()
    restaurant_query="""
        COPY
            (
                SELECT re.restaurant_id, 
                        re.restaurant_name, 
                        re.restaurant_link, 
                        re.restaurant_halal_certificate, 
                        re.restaurant_address, 
                        re.latitude, 
                        re.longitude, 
                        re.restaurant_contact, 
                        r.reviews  FROM restaurant re 
                LEFT JOIN (SELECT restaurant_id, 
                                string_agg(comment_text, '\n\n\n') as reviews 
                            FROM reviews GROUP BY restaurant_id) r 
                ON re.restaurant_id = r.restaurant_id
            )
        TO 'data/raw/text/restaurants.parquet'
        (FORMAT parquet);
    """
    db_conn.execute(restaurant_query)
    
    reviews_query="""
        COPY
            (
                SELECT DISTINCT restaurant_id, comment_text
                FROM reviews
            )
        TO 'data/raw/text/reviews.parquet'
        (FORMAT parquet);
    """
    db_conn.execute(reviews_query)

def backup_data():
    shutil.copyfile('./data/restaurants.duckdb', './data/backup/restaurants.duckdb')
    pass 

def get_duplicated_restaurants(restaurant_links):
    df_links = pd.read_csv(restaurant_links)
    query = """
        with get_duplicated as (
            select 
                restaurant_name,
                google_maps_link,
                row_number() over (partition by restaurant_name order by google_maps_link) as rn
            from df_links
        )
        select 
            restaurant_name,
            google_maps_link
        from get_duplicated
        where rn > 1
    """
    duplicated_restaurants = pdsql.sqldf(query, locals())
    return 

def load_duplicated_restaurants(db_conn):
    query = f"""
        with get_duplicated as (
            select
            restaurant_name,
            restaurant_id,
            row_number() over (partition by restaurant_name order by restaurant_id) rn
            from restaurant
            ) select restaurant_name, restaurant_id from get_duplicated where rn > 1;
    """