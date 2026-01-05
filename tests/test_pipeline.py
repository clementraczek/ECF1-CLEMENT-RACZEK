"""
Tests unitaires pour le pipeline e-commerce.

Usage:
    pytest tests/
    pytest tests/test_pipeline.py -v
"""

import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.ingestion import Product


class TestProduct:
    """Tests pour la classe Product."""
    
    def test_product_creation(self):
        """Test création d'un produit."""
        product = Product(
            title="Test Laptop",
            price=999.99,
            description="A test laptop",
            rating=4,
            reviews_count=10,
            image_url="http://example.com/image.jpg",
            product_url="http://example.com/product",
            category="computers",
            subcategory="laptops"
        )
        
        assert product.title == "Test Laptop"
        assert product.price == 999.99
        assert product.rating == 4
        assert product.category == "computers"
    
    def test_product_sku_generation(self):
        """Test génération du SKU."""
        product = Product(
            title="Test Product",
            price=100,
            description="",
            rating=3,
            reviews_count=0,
            image_url="",
            product_url=""
        )
        
        # Le SKU doit être un hash MD5 tronqué à 12 caractères
        assert len(product.sku) == 12
        assert product.sku.isupper()
    
    def test_product_to_dict(self):
        """Test conversion en dictionnaire."""
        product = Product(
            title="Test Product",
            price=199.99,
            description="Description",
            rating=5,
            reviews_count=20,
            image_url="http://example.com/img.jpg",
            product_url="http://example.com/prod",
            category="phones",
            subcategory="touch"
        )
        
        data = product.to_dict()
        
        assert "sku" in data
        assert data["title"] == "Test Product"
        assert data["price"] == 199.99
        assert data["rating"] == 5
        assert data["category"] == "phones"
        # image_data ne doit pas être dans le dict
        assert "image_data" not in data


class TestScraperHelpers:
    """Tests pour les fonctions utilitaires du scraper."""
    
    def test_parse_price_valid(self):
        """Test parsing de prix valides."""
        from src.ingestion.ecommerce_scraper import EcommerceScraper
        
        scraper = EcommerceScraper()
        
        assert scraper._parse_price("$999.99") == 999.99
        assert scraper._parse_price("$1,299.00") == 1299.00
        assert scraper._parse_price("€50.00") == 50.00
        assert scraper._parse_price("Price: $42") == 42.0
        
        scraper.close()
    
    def test_parse_price_invalid(self):
        """Test parsing de prix invalides."""
        from src.ingestion.ecommerce_scraper import EcommerceScraper
        
        scraper = EcommerceScraper()
        
        assert scraper._parse_price("") == 0.0
        assert scraper._parse_price("N/A") == 0.0
        assert scraper._parse_price("Free") == 0.0
        
        scraper.close()
    
    def test_parse_reviews(self):
        """Test parsing du nombre de reviews."""
        from src.ingestion.ecommerce_scraper import EcommerceScraper
        
        scraper = EcommerceScraper()
        
        assert scraper._parse_reviews("10 reviews") == 10
        assert scraper._parse_reviews("1 review") == 1
        assert scraper._parse_reviews("No reviews") == 0
        assert scraper._parse_reviews("") == 0
        
        scraper.close()


class TestConfiguration:
    """Tests pour la configuration."""
    
    def test_minio_config(self):
        """Test configuration MinIO."""
        from config.settings import minio_config
        
        assert minio_config.endpoint == "localhost:9000"
        assert minio_config.secure == False
        assert minio_config.bucket_images == "product-images"
    
    def test_mongo_config(self):
        """Test configuration MongoDB."""
        from config.settings import mongo_config
        
        assert mongo_config.host == "localhost"
        assert mongo_config.port == 27017
        assert "mongodb://" in mongo_config.connection_string
    
    def test_scraper_config(self):
        """Test configuration scraper."""
        from config.settings import scraper_config
        
        assert "webscraper.io" in scraper_config.base_url
        assert scraper_config.delay >= 0
        assert scraper_config.max_retries > 0


# Tests d'intégration (nécessitent Docker)
@pytest.mark.integration
class TestIntegration:
    """Tests d'intégration nécessitant l'infrastructure Docker."""
    
    @pytest.fixture
    def minio_client(self):
        """Fixture pour le client MinIO."""
        from src.storage import MinIOStorage
        client = MinIOStorage()
        yield client
    
    @pytest.fixture
    def mongo_client(self):
        """Fixture pour le client MongoDB."""
        from src.storage import MongoDBStorage
        client = MongoDBStorage()
        yield client
        client.close()
    
    def test_minio_connection(self, minio_client):
        """Test connexion MinIO."""
        # Doit pouvoir lister les buckets sans erreur
        stats = minio_client.get_stats()
        assert isinstance(stats, dict)
    
    def test_mongo_connection(self, mongo_client):
        """Test connexion MongoDB."""
        # Doit pouvoir compter les documents
        count = mongo_client.count_products()
        assert isinstance(count, int)
    
    def test_minio_upload_download(self, minio_client):
        """Test upload/download MinIO."""
        test_data = b"Test data content"
        filename = "test_upload.txt"
        
        # Upload
        uri = minio_client.upload_export(test_data, filename, "text/plain")
        assert uri is not None
        
        # Download
        downloaded = minio_client.get_export(filename)
        assert downloaded == test_data
    
    def test_mongo_upsert_find(self, mongo_client):
        """Test insert/find MongoDB."""
        test_product = {
            "title": "Integration Test Product",
            "price": 123.45,
            "rating": 3,
            "category": "test",
            "subcategory": "integration"
        }
        
        # Insert
        result = mongo_client.upsert_product(test_product)
        assert result is not None
        
        # Find
        products = mongo_client.find_products({"title": "Integration Test Product"})
        assert len(products) > 0
        assert products[0]["price"] == 123.45
        
        # Cleanup
        mongo_client.products.delete_one({"title": "Integration Test Product"})


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
