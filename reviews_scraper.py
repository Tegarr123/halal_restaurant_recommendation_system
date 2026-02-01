from utils import *


def main():
    restaurant_links = get_restaurant_links()
    db_conn = get_restaurants_db()
    
    for row in restaurant_links:
        if is_restaurant_scraped(db_conn, extract_link(row.google_maps_link)):
            print(f"Skipping already scraped restaurant: {row.restaurant_name}")
            continue
        print(f"Scraping restaurant: {row.restaurant_name}")
        driver = init_firefox_driver()
        open_restaurant_page(driver, row.google_maps_link)
        new_restaurant = get_restaurants_information(driver, row)
        
        reviews_collected = scrape_all_reviews(driver)
        insert_restaurant_information(db_conn, new_restaurant)
        insert_reviews_information(db_conn, new_restaurant['restaurant_id'], reviews_collected)
        driver.quit()
        
if __name__ == "__main__":
    main()