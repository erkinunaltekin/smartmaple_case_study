import psycopg2
from psycopg2.extras import execute_values
import os
from src.models.campground import Campground

DATABASE_URL = os.getenv("DB_URL")

def connect_db():
    return psycopg2.connect(DATABASE_URL)

def create_table():
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS campgrounds (
            id TEXT PRIMARY KEY,
            type TEXT,
            name TEXT,
            latitude FLOAT,
            longitude FLOAT,
            region_name TEXT,
            administrative_area TEXT,
            nearest_city_name TEXT,
            accommodation_type_names TEXT[],
            bookable BOOLEAN,
            camper_types TEXT[],
            operator TEXT,
            photo_url TEXT,
            photo_urls TEXT[],
            photos_count INTEGER,
            rating FLOAT,
            reviews_count INTEGER,
            slug TEXT,
            price_low FLOAT,
            price_high FLOAT,
            availability_updated_at TIMESTAMP,
            address TEXT
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

def insert_campgrounds(campgrounds: list[Campground]):
    conn = connect_db()
    cur = conn.cursor()

    records = [
        (
            c.id,
            c.type,
            c.attributes.name,
            c.attributes.latitude,
            c.attributes.longitude,
            c.attributes.region_name,
            c.attributes.administrative_area,
            c.attributes.nearest_city_name,
            c.attributes.accommodation_type_names,
            c.attributes.bookable,
            c.attributes.camper_types,
            c.attributes.operator,
            str(c.attributes.photo_url) if c.attributes.photo_url else None,
            [str(url) for url in c.attributes.photo_urls],
            c.attributes.photos_count,
            c.attributes.rating,
            c.attributes.reviews_count,
            c.attributes.slug,
            c.attributes.price_low,
            c.attributes.price_high,
            c.attributes.availability_updated_at,
            c.attributes.address
        )
        for c in campgrounds
    ]

    insert_query = """
    INSERT INTO campgrounds (
        id, type, name, latitude, longitude, region_name, administrative_area,
        nearest_city_name, accommodation_type_names, bookable, camper_types,
        operator, photo_url, photo_urls, photos_count, rating, reviews_count,
        slug, price_low, price_high, availability_updated_at, address
    )
    VALUES %s
    ON CONFLICT (id) DO UPDATE SET
        type = EXCLUDED.type,
        name = EXCLUDED.name,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        region_name = EXCLUDED.region_name,
        administrative_area = EXCLUDED.administrative_area,
        nearest_city_name = EXCLUDED.nearest_city_name,
        accommodation_type_names = EXCLUDED.accommodation_type_names,
        bookable = EXCLUDED.bookable,
        camper_types = EXCLUDED.camper_types,
        operator = EXCLUDED.operator,
        photo_url = EXCLUDED.photo_url,
        photo_urls = EXCLUDED.photo_urls,
        photos_count = EXCLUDED.photos_count,
        rating = EXCLUDED.rating,
        reviews_count = EXCLUDED.reviews_count,
        slug = EXCLUDED.slug,
        price_low = EXCLUDED.price_low,
        price_high = EXCLUDED.price_high,
        availability_updated_at = EXCLUDED.availability_updated_at,
        address = EXCLUDED.address
    """
    execute_values(cur, insert_query, records)
    conn.commit()
    cur.close()
    conn.close()
