import re
from datetime import datetime
import pandas as pd
import scrapy
from items import QuoteItem  # On peut renommer en QuoteItem si tu veux
from src.storage.minio_client import MinioClient
import uuid


class QuotesSpider(scrapy.Spider):
    name = "quotes"
    allowed_domains = ["quotes.toscrape.com"]
    start_urls = ["https://quotes.toscrape.com/page/1/"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.all_quotes = []
        self.client = MinioClient()  # ton client MinIO existant

    def parse(self, response):
        # Scraper toutes les citations de la page
        for quote in response.css('div.quote'):
            item = QuoteItem()  # ou QuoteItem si tu crées un item dédié
            item['id'] = str(uuid.uuid4())
            item['text'] = quote.css('span.text::text').get()
            item['author'] = quote.css('small.author::text').get()
            item['tags'] = ",".join(quote.css('div.tags a.tag::text').getall())
            self.all_quotes.append(dict(item))
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
        if not self.all_quotes:
            self.logger.warning("Aucune citation à uploader.")
            return

        # Crée le DataFrame
        df = pd.DataFrame(self.all_quotes)
        csv_content = df.to_csv(index=False)  # CSV en mémoire

        # Nom du fichier CSV avec seulement date et heure
        filename = f"quotes_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
        object_path = f"scraping/quotes/{filename}"

        # Upload vers le bucket bronze
        uri = self.client.upload_csv(csv_content, object_path)

        if uri:
            self.logger.info(f"Fichier uploadé avec succès : {uri}")
            print(f"Fichier uploadé avec succès : {uri}")
        else:
            self.logger.error(f"Échec de l'upload du fichier : {filename}")
            print(f"Échec de l'upload du fichier : {filename}")
