import re
import time
import hashlib
import pandas as pd
import uuid
from datetime import datetime
from typing import Optional
from urllib.parse import urljoin
from dataclasses import dataclass

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential
import structlog

from config.settings import scraper_config
from src.storage.minio_client import MinioClient

logger = structlog.get_logger()

@dataclass
class Product:
    title: str
    price: float
    description: str
    rating: int
    reviews_count: int
    image_url: str
    product_url: str
    category: str = ""
    subcategory: str = ""
    
    @property
    def sku(self) -> str:
        return hashlib.md5(self.title.encode()).hexdigest()[:12].upper()
    
    def to_dict(self) -> dict:
        d = {k: v for k, v in self.__dict__.items()}
        d['sku'] = self.sku
        d['id'] = str(uuid.uuid4())
        return d

class EcommerceScraper:
    CATEGORIES = {
        "computers": {
            "laptops": "/computers/laptops",
            "tablets": "/computers/tablets"
        },
        "phones": {
            "touch": "/phones/touch"
        }
    }
    
    def __init__(self):
        self.base_url = "https://webscraper.io/test-sites/e-commerce/allinone"
        self.delay = max(1.0, scraper_config.delay)
        self.session = requests.Session()
        self.client = MinioClient()
        self.session.headers.update({"User-Agent": scraper_config.user_agent})
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _fetch(self, url: str) -> Optional[BeautifulSoup]:
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            time.sleep(self.delay)
            return BeautifulSoup(response.content, "lxml")
        except Exception as e:
            logger.error("fetch_error", url=url, error=str(e))
            raise

    def scrape_all_and_upload(self):
        all_products = []
        
        for cat, subcats in self.CATEGORIES.items():
            for subcat, path in subcats.items():
                current_url = self.base_url + path
                page_count = 0
                
                while current_url and page_count < scraper_config.max_pages:
                    logger.info("scraping_page", url=current_url)
                    soup = self._fetch(current_url)
                    
                    if not soup:
                        break
                        
                    items = soup.find_all("div", class_="thumbnail")
                    for item in items:
                        prod = self._parse_product(item, cat, subcat)
                        if prod:
                            all_products.append(prod.to_dict())
                    
                    next_link = soup.select_one('ul.pagination li.active + li a')
                    if next_link and next_link.get('href'):
                        current_url = urljoin(self.base_url, next_link['href'])
                        page_count += 1
                    else:
                        current_url = None
        
        if all_products:
            self._upload_to_minio(all_products)

    def _parse_product(self, element, category, subcategory) -> Optional[Product]:
        try:
            title_elem = element.find("a", class_="title")
            price_elem = element.find("h4", class_="price")
            desc_elem = element.find("p", class_="description")
            rev_elem = element.find("p", class_="review-count")
            rating_div = element.find("div", class_="ratings")
            rating = len(rating_div.find_all("span", class_="glyphicon-star")) if rating_div else 0

            return Product(
                title=title_elem.get("title") or title_elem.text.strip(),
                price=float(re.search(r"[\d.]+", price_elem.text).group().replace(",", "")),
                description=desc_elem.text.strip(),
                rating=rating,
                reviews_count=int(re.search(r'\d+', rev_elem.text).group()) if rev_elem else 0,
                image_url=urljoin("https://webscraper.io", element.find("img")["src"]),
                product_url=urljoin(self.base_url, title_elem["href"]),
                category=category,
                subcategory=subcategory
            )
        except Exception as e:
            logger.error("parse_error", error=str(e))
            return None

    def _upload_to_minio(self, data: list):
        df = pd.DataFrame(data)
        csv_content = df.to_csv(index=False)
        filename = f"ecommerce_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        object_path = f"scraping/ecommerce/{filename}"
        
        if self.client.upload_csv(csv_content, object_path):
            print(f"Status: Export success | Records: {len(data)} | File: {filename}")

    def close(self):
        self.session.close()

if __name__ == "__main__":
    scraper = EcommerceScraper()
    try:
        scraper.scrape_all_and_upload()
    finally:
        scraper.close()