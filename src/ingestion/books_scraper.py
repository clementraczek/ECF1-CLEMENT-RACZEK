import re
from datetime import datetime

import pandas as pd
import scrapy
from ingestion.items import BookItem
from storage.minio_client import MinioClient
import uuid



class BooksSpider(scrapy.Spider):
    name = "books"
    allowed_domains = ["books.toscrape.com"]
    start_urls = ["https://books.toscrape.com/catalogue/page-1.html"]

    rating_map = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.all_books = []
        self.client = MinioClient()  # ton client MinIO existant

    def parse(self, response):
        # Scraper tous les livres de la page
        for book in response.css('article.product_pod'):
            item = BookItem()
            item['id'] = str(uuid.uuid4())
            item['title'] = book.css('h3 a::attr(title)').get()
            price_text = book.css('p.price_color::text').get()
            item['price_gbp'] = float(re.findall(r'[\d.]+', price_text)[0])
            rating_class = book.css('p.star-rating::attr(class)').get()
            rating_text = rating_class.split()[1]
            item['rating'] = self.rating_map.get(rating_text, 0)
            availability = book.css('p.instock.availability::text').getall()
            item['availability'] = availability[1].strip() if len(availability) > 1 else 'unknown'
            self.all_books.append(dict(item))
            yield item

        # Pagination : suivre la page suivante si elle existe
        next_page = response.css('li.next a::attr(href)').get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)
        else:
            # Fin du crawl → upload unique sur MinIO
            self.upload_to_minio()

    def upload_to_minio(self):
        """
        Génère le CSV en mémoire et l'envoie sur le bucket bronze de Minio.
        """
        if not self.all_books:
            self.logger.warning("Aucun livre à uploader.")
            return

        # Crée le DataFrame
        df = pd.DataFrame(self.all_books)
        csv_content = df.to_csv(index=False)  # CSV en mémoire

        # Nom du fichier CSV avec seulement date et heure
        filename = f"books_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
        object_path = f"scraping/books/{filename}"

        # Upload vers le bucket bronze
        uri = self.client.upload_csv(csv_content, object_path)

        if uri:
            self.logger.info(f"Fichier uploadé avec succès : {uri}")
            print(f"Fichier uploadé avec succès : {uri}")
        else:
            self.logger.error(f"Échec de l'upload du fichier : {filename}")
            print(f"Échec de l'upload du fichier : {filename}")
