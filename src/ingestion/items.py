import scrapy

class BookstoreItem(scrapy.Item):
    pass

class BookItem(scrapy.Item):
    id = scrapy.Field() 
    title = scrapy.Field()
    price = scrapy.Field()
    rating = scrapy.Field()
    availability = scrapy.Field()
    price_gbp = scrapy.Field()

class QuoteItem(scrapy.Item):
    id = scrapy.Field()
    text = scrapy.Field()
    author = scrapy.Field()
    tags = scrapy.Field()

class EcommerceItem(scrapy.Item):
    name = scrapy.Field()
    price = scrapy.Field()
    description = scrapy.Field()
    category = scrapy.Field()