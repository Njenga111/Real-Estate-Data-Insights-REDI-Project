# etl/extract/extractor.py

import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
from tenacity import retry, stop_after_attempt, wait_exponential
import os
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Website configurations with flexible selectors
SOURCES_CONFIG = {
    'property254': {
        'url': 'https://property254.co.ke/properties',
        'listing_selector': 'div.property-item',  # Main listing container
        'title_selector': 'h4.property-title a',  # Title element
        'price_selector': 'span.property-price',  # Price element
        'location_selector': 'div.property-location',  # Location element
        'required_fields': ['title', 'price', 'location']
    },
    # ... (keep buyrentkenya config as is) ...

    
    'buyrentkenya': {
        'url': 'https://buyrentkenya.com',
        'listing_selector': 'div.property-item',
        'title_selector': 'h4.property-title a',
        'price_selector': 'div.price-wrapper',
        'location_selector': 'div.location-meta',
        'required_fields': ['title', 'price', 'location']
    }
}

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_url(url):
    """Fetch URL with retry logic and anti-scraping headers"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/"
    }
    
    response = requests.get(
        url,
        headers=headers,
        timeout=15
    )
    response.raise_for_status()
    return response

def save_debug_html(source, content):
    """Save HTML for debugging"""
    debug_dir = "debug_html"
    os.makedirs(debug_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{debug_dir}/{source}_{timestamp}.html"
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(content)
    return filename

def extract_listing_data(listing, config):
    """Extract data from a single listing with error handling"""
    data = {'source': config['source_name']}
    
    try:
        data['title'] = listing.select_one(config['title_selector']).get_text(strip=True)
    except (AttributeError, TypeError):
        data['title'] = None
        
    try:
        price_text = listing.select_one(config['price_selector']).get_text(strip=True)
        data['price'] = ''.join(filter(str.isdigit, price_text)) or None
    except (AttributeError, TypeError):
        data['price'] = None
        
    try:
        data['location'] = listing.select_one(config['location_selector']).get_text(strip=True)
    except (AttributeError, TypeError):
        data['location'] = None
        
    return data

def extract_data_from_websites(source):
        
    if source not in SOURCES_CONFIG:
        raise ValueError(f"Unknown source: {source}. Available sources: {list(SOURCES_CONFIG.keys())}")
    
    config = SOURCES_CONFIG[source]
    config['source_name'] = source  # Add source name to config
    
    try:
        logger.info(f"Extracting data from {source}...")
        response = fetch_url(config['url'])
        html_file = save_debug_html(source, response.text)
        logger.debug(f"Saved debug HTML to {html_file}")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        listings = soup.select(config['listing_selector'])
        logger.info(f"Found {len(listings)} listings")
        
        if not listings:
            raise ValueError(f"No listings found using selector: {config['listing_selector']}")
        
        data = [extract_listing_data(listing, config) for listing in listings]
        df = pd.DataFrame(data)
        
        # Validate required fields
        missing_fields = [field for field in config['required_fields'] if field not in df.columns]
        if missing_fields:
            raise ValueError(f"Missing required fields: {missing_fields}")
            
        logger.info(f"Successfully extracted {len(df)} records from {source}")
        return df.dropna(subset=config['required_fields'])
        
    except Exception as e:
        logger.error(f"Failed to extract from {source}: {str(e)}")
        raise