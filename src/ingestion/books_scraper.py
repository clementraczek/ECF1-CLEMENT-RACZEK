import re
import uuid
import scrapy
import pandas as pd
from datetime import datetime
from config.settings import scraper_config
from src.storage.minio_client import MinioClient

try:
    from items import BookItem
except ImportError:
    BookItem = dict

class BooksSpider(scrapy.Spider):
    name = "books"
    allowed_domains = ["books.toscrape.com"]
    start_urls = ["https://books.toscrape.com/catalogue/page-1.html"]

    custom_settings = {
        'DOWNLOAD_DELAY': max(1.0, scraper_config.delay),
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'USER_AGENT': scraper_config.user_agent,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'HTTPERROR_ALLOWED_CODES': [404, 500, 503],
    }

    rating_map = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.page_count = 0
        self.max_pages = scraper_config.max_pages
        self.all_books = []
        self.client = MinioClient()

    def parse(self, response):
        if response.status != 200:
            self.logger.error(f"HTTP_ERROR: {response.status} | URL: {response.url}")
            return

        self.logger.info(f"Parsing: {response.url}")

        for book in response.css('article.product_pod'):
            item = BookItem()
            item['title'] = book.css('h3 a::attr(title)').get()
            
            price_text = book.css('p.price_color::text').get()
            if price_text:
                item['price_gbp'] = float(re.findall(r'[\d.]+', price_text)[0])
            
            rating_class = book.css('p.star-rating::attr(class)').get()
            rating_text = rating_class.split()[1] if rating_class else 'Zero'
            item['rating'] = self.rating_map.get(rating_text, 0)
            
            availability = book.css('p.instock.availability::text').getall()
            item['availability'] = availability[1].strip() if len(availability) > 1 else 'unknown'
            item['id'] = str(uuid.uuid4())
            
            self.all_books.append(dict(item))

        self.page_count += 1
        next_page = response.css('li.next a::attr(href)').get()

        if next_page and self.page_count < self.max_pages:
            yield response.follow(next_page, callback=self.parse)
        else:
            self.upload_to_minio()

    def upload_to_minio(self):
        if not self.all_books:
            self.logger.warning("Status: No data extracted")
            return

        df = pd.DataFrame(self.all_books)
        csv_content = df.to_csv(index=False)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"books_{timestamp}_{str(uuid.uuid4())[:8]}.csv"
        object_path = f"scraping/books/{filename}"

        uri = self.client.upload_csv(csv_content, object_path)
        if uri:
            self.logger.info(f"Export_status: Success | URI: {uri}")
        else:
            self.logger.error("Export_status: Failure")