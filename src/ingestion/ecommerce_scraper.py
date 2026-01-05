import re
import time
import hashlib
import pandas as pd
from datetime import datetime
from typing import Optional, Generator
from urllib.parse import urljoin
from dataclasses import dataclass, field

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from tenacity import retry, stop_after_attempt, wait_exponential
import structlog

# Imports de ton projet
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
    specs: dict = field(default_factory=dict)
    
    @property
    def sku(self) -> str:
        return hashlib.md5(self.title.encode()).hexdigest()[:12].upper()
    
    def to_dict(self) -> dict:
        d = {k: v for k, v in self.__dict__.items() if k != 'specs'}
        d['sku'] = self.sku
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
        self.delay = scraper_config.delay
        self.session = requests.Session()
        self.ua = UserAgent()
        self.client = MinioClient() # Connexion à MinIO
        self._setup_session()
    
    def _setup_session(self) -> None:
        self.session.headers.update({"User-Agent": self.ua.random})
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _fetch(self, url: str) -> Optional[BeautifulSoup]:
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            time.sleep(self.delay)
            return BeautifulSoup(response.content, "lxml")
        except Exception as e:
            logger.error("fetch_failed", url=url, error=str(e))
            raise

    def _parse_price(self, text: str) -> float:
        text = text.replace(",", "")
        match = re.search(r"[\d.]+", text)
        return float(match.group()) if match else 0.0

    def scrape_all_and_upload(self):
        """Méthode principale pour scraper et envoyer vers MinIO."""
        all_products = []
        
        for cat, subcats in self.CATEGORIES.items():
            for subcat, path in subcats.items():
                url = self.base_url + path
                logger.info("scraping_category", category=cat, subcategory=subcat)
                
                soup = self._fetch(url)
                if soup:
                    items = soup.find_all("div", class_="thumbnail")
                    for item in items:
                        prod = self._parse_product(item, cat, subcat)
                        if prod:
                            all_products.append(prod.to_dict())

        if all_products:
            self._upload_to_minio(all_products)
        else:
            logger.warning("no_products_to_upload")

    def _parse_product(self, element, category, subcategory) -> Product:
        try:
            title_elem = element.find("a", class_="title")
            price_elem = element.find("h4", class_="price")
            desc_elem = element.find("p", class_="description")
            rev_elem = element.find("p", class_="review-count")
            
            # Extraction simple des étoiles via la classe des icônes
            rating_div = element.find("div", class_="ratings")
            rating = len(rating_div.find_all("span", class_="glyphicon-star")) if rating_div else 0

            return Product(
                title=title_elem.get("title") or title_elem.text.strip(),
                price=self._parse_price(price_elem.text),
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
        """Génère le CSV et l'envoie sur le bucket Bronze."""
        df = pd.DataFrame(data)
        csv_content = df.to_csv(index=False)
        
        filename = f"ecommerce_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        object_path = f"scraping/ecommerce/{filename}"
        
        uri = self.client.upload_csv(csv_content, object_path)
        if uri:
            logger.info("upload_success", uri=uri)
            print(f"✅ Données e-commerce envoyées vers : {uri}")

    def close(self):
        self.session.close()

if __name__ == "__main__":
    scraper = EcommerceScraper()
    scraper.scrape_all_and_upload()
    scraper.close()