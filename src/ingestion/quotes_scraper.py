import uuid
import scrapy
import pandas as pd
from datetime import datetime
from config.settings import scraper_config
from src.storage.minio_client import MinioClient

try:
    from items import QuoteItem
except ImportError:
    QuoteItem = dict

class QuotesSpider(scrapy.Spider):
    name = "quotes"
    allowed_domains = ["quotes.toscrape.com"]
    start_urls = ["https://quotes.toscrape.com/page/1/"]

    custom_settings = {
        'DOWNLOAD_DELAY': max(1.0, scraper_config.delay),
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'USER_AGENT': scraper_config.user_agent,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'HTTPERROR_ALLOWED_CODES': [404, 500, 503],
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.page_count = 0
        self.max_pages = scraper_config.max_pages
        self.all_quotes = []
        self.client = MinioClient()

    def parse(self, response):
        if response.status != 200:
            self.logger.error(f"HTTP_ERROR: {response.status} | URL: {response.url}")
            return

        self.logger.info(f"Parsing: {response.url}")

        for quote in response.css('div.quote'):
            item = QuoteItem()
            item['id'] = str(uuid.uuid4())
            item['text'] = quote.css('span.text::text').get()
            item['author'] = quote.css('small.author::text').get()
            item['tags'] = ",".join(quote.css('div.tags a.tag::text').getall())
            self.all_quotes.append(dict(item))

        self.page_count += 1
        next_page = response.css('li.next a::attr(href)').get()

        if next_page and self.page_count < self.max_pages:
            yield response.follow(next_page, callback=self.parse)
        else:
            self.upload_to_minio()

    def upload_to_minio(self):
        if not self.all_quotes:
            self.logger.warning("Status: No data extracted")
            return

        df = pd.DataFrame(self.all_quotes)
        csv_content = df.to_csv(index=False)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"quotes_{timestamp}.csv"
        object_path = f"scraping/quotes/{filename}"

        uri = self.client.upload_csv(csv_content, object_path)

        if uri:
            self.logger.info(f"Export_status: Success | URI: {uri}")
        else:
            self.logger.error("Export_status: Failure")