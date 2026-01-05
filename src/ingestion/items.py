# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class BookstoreItem(scrapy.Item):
    """
    Item générique pour la librairie.
    Peut servir de base ou extension pour d'autres items.
    """
    pass


# ==========================
# 1. BookItem
# ==========================
# Pour books.toscrape.com
class BookItem(scrapy.Item):
    id = scrapy.Field() 
    title = scrapy.Field()
    price = scrapy.Field()
    rating = scrapy.Field()
    availability = scrapy.Field()
    price_gbp = scrapy.Field()
    



# ==========================
# 2. QuoteItem
# ==========================
# Pour quotes.toscrape.com
class QuoteItem(scrapy.Item):
    text = scrapy.Field()
    author = scrapy.Field()
    tags = scrapy.Field()


# ==========================
# 3. EcommerceItem (optionnel)
# ==========================
# Pour le site e-commerce de test
class EcommerceItem(scrapy.Item):
    name = scrapy.Field()
    price = scrapy.Field()
    description = scrapy.Field()
    category = scrapy.Field()
