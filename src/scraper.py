import requests
from typing import List
from src.models.campground import Campground
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time

logging.basicConfig(level=logging.INFO)

API_URL = "https://thedyrt.com/api/v6/locations/search-results?filter%5Bsearch%5D%5Bdrive_time%5D=any&filter%5Bsearch%5D%5Bair_quality%5D=any&filter%5Bsearch%5D%5Belectric_amperage%5D=any&filter%5Bsearch%5D%5Bmax_vehicle_length%5D=any&filter%5Bsearch%5D%5Bprice%5D=any&filter%5Bsearch%5D%5Brating%5D=any&filter%5Bsearch%5D%5Bbbox%5D=-123.154%2C38.889%2C-97.609%2C48.857&sort=recommended&page%5Bnumber%5D=1&page%5Bsize%5D=500"

geolocator = Nominatim(user_agent="campground-scraper")

def reverse_geocode(lat, lon):
    try:
        location = geolocator.reverse((lat, lon), timeout=10)
        return location.address if location else None
    except GeocoderTimedOut:
        #time.sleep(0.5)
        return reverse_geocode(lat, lon)

def fetch_campgrounds() -> List[Campground]:
    print("Veri çekiliyor")

    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    try:
        response = session.get(API_URL)
        print(f"Status Code: {response.status_code}")
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"İstek başarısız oldu: {e}")
        return []

    try:
        data = response.json()
    except Exception as e:
        logging.error(f"JSON parse hatası: {e}")
        return []

    items = data.get("data", [])
    print(f"Toplam {len(items)} sonuç bulundu")

    campgrounds = []
    for item in items:
        try:
            campground = Campground(**item)
            if not campground.attributes.address:
                campground.attributes.address = reverse_geocode(
                    campground.attributes.latitude,
                    campground.attributes.longitude
                )
                time.sleep(0.3)
            
            campgrounds.append(campground)
        except Exception as e:
            logging.warning(f"Hatalı veri: {e}")
            continue

    return campgrounds
