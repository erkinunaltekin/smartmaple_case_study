"""
Main entrypoint for The Dyrt web scraper case study.

Usage:
    The scraper can be run directly (`python main.py`) or via Docker Compose (`docker compose up`).

If you have any questions in mind you can connect to me directly via info@smart-maple.com
"""


from src.scraper import fetch_campgrounds
import schedule
import time
from src.scraper import fetch_campgrounds
from db import create_table, insert_campgrounds

def job():
    campgrounds = fetch_campgrounds()
    insert_campgrounds(campgrounds)

if __name__ == "__main__":
    print("Başlatılıyor")
    create_table()
    job()  

    
    schedule.every(10).minutes.do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)


