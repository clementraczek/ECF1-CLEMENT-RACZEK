import re
import os
import uuid
from datetime import datetime

import pandas as pd
import scrapy
from items import BookItem
from src.storage.minio_client import MinioClient


class BooksSpider(scrapy.Spider):
    name = "books"
    allowed_domains = ["books.toscrape.com"]
    start_urls = ["https://books.toscrape.com/catalogue/page-1.html"]

    rating_map = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}
    max_pages = 3  # Limite de pages à scraper

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.page_count = 0
        self.all_books = []
        self.client = MinioClient()  # Assure-toi que ton MinioClient a upload_csv()

    def parse(self, response):
        for book in response.css('article.product_pod'):
            item = BookItem()
            item['title'] = book.css('h3 a::attr(title)').get()
            price_text = book.css('p.price_color::text').get()
            item['price_gbp'] = float(re.findall(r'[\d.]+', price_text)[0])
            rating_class = book.css('p.star-rating::attr(class)').get()
            rating_text = rating_class.split()[1]
            item['rating'] = self.rating_map.get(rating_text, 0)
            availability = book.css('p.instock.availability::text').getall()
            item['availability'] = availability[1].strip() if len(availability) > 1 else 'unknown'
            item['id'] = str(uuid.uuid4())
            self.all_books.append(dict(item))
            yield item

        # Pagination
        self.page_count += 1
        if self.page_count < self.max_pages:
            next_page = response.css('li.next a::attr(href)').get()
            if next_page:
                yield response.follow(next_page, callback=self.parse)
        else:
            self.upload_to_minio()

    def upload_to_minio(self):
        """
        Génère le CSV en mémoire et l'envoie sur le bucket par défaut de Minio.
        """
        if not self.all_books:
            self.logger.warning("Aucun livre à uploader.")
            return

        # Crée le DataFrame
        df = pd.DataFrame(self.all_books)
        csv_content = df.to_csv(index=False)  # Contenu CSV en mémoire

        # Nom du fichier CSV unique
        filename = f"books_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4()}.csv"
        object_path = f"scraping/books/{filename}"

        # Upload vers le bucket par défaut (bronze)
        uri = self.client.upload_csv(csv_content, object_path)

        if uri:
            self.logger.info(f"Fichier uploadé avec succès : {uri}")
            print(f"Fichier uploadé avec succès : {uri}")
        else:
            self.logger.error(f"Échec de l'upload du fichier : {filename}")
            print(f"Échec de l'upload du fichier : {filename}")
