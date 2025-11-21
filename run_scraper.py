import os
import sys
from ladbrokes_racing_scraper import LadbrokesRacingScraper

# Get configuration from environment
email = os.getenv('SCRAPER_EMAIL')
partner = os.getenv('SCRAPER_PARTNER')
date = os.getenv('SCRAPE_DATE')
countries_str = os.getenv('SCRAPE_COUNTRIES', 'AUS')
categories_str = os.getenv('SCRAPE_CATEGORIES', 'T,G')

# Parse countries and categories
if countries_str == 'ALL':
    countries = 'ALL'
else:
    countries = [c.strip() for c in countries_str.split(',')]

categories = [c.strip() for c in categories_str.split(',')]

print(f"Scraping with configuration:")
print(f"  Date: {date}")
print(f"  Countries: {countries}")
print(f"  Categories: {categories}")

# Run scraper
scraper = LadbrokesRacingScraper(email, partner)
scraper.scrape_and_save(
    date=date,
    interactive=False,
    countries=countries,
    categories=categories
)
