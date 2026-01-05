"""
Client MongoDB pour les données e-commerce.

Ce module fournit une interface pour stocker et requêter :
- Les produits (métadonnées, prix, ratings)
- L'historique des prix
- Les logs de scraping
- Les statistiques et agrégations

MongoDB est une base de données documentaire NoSQL.
"""

from datetime import datetime, timedelta
from typing import Optional, Any
from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT
from pymongo.errors import PyMongoError
from bson import ObjectId
import structlog

# Import de la configuration
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from config.settings import mongo_config

logger = structlog.get_logger()


class MongoDBStorage:
    """
    Gestionnaire MongoDB pour les données e-commerce.
    
    Collections gérées :
    - products : Catalogue de produits
    - price_history : Historique des prix pour tracking
    - scraping_logs : Logs d'exécution du scraper
    
    Attributes:
        client: Client MongoDB
        db: Base de données
        products: Collection des produits
        price_history: Collection de l'historique
        scraping_logs: Collection des logs
    
    Example:
        >>> mongo = MongoDBStorage()
        >>> mongo.upsert_product({"title": "Laptop", "price": 999.99})
        >>> products = mongo.find_products({"price": {"$lt": 500}})
    """
    
    def __init__(self):
        """Initialise la connexion MongoDB et crée les index."""
        self.client = MongoClient(mongo_config.connection_string)
        self.db = self.client[mongo_config.database]
        
        # Collections
        self.products = self.db["products"]
        self.price_history = self.db["price_history"]
        self.scraping_logs = self.db["scraping_logs"]
        
        # Créer les index
        self._create_indexes()
    
    def _create_indexes(self) -> None:
        """
        Crée les index pour optimiser les requêtes.
        
        Index créés :
        - Recherche full-text sur title et description
        - Index sur category, subcategory, price, rating
        - Index unique sur SKU
        """
        try:
            # Index full-text pour recherche
            self.products.create_index([("title", TEXT), ("description", TEXT)])
            
            # Index pour filtres et tri
            self.products.create_index([("category", ASCENDING)])
            self.products.create_index([("subcategory", ASCENDING)])
            self.products.create_index([("price", ASCENDING)])
            self.products.create_index([("rating", DESCENDING)])
            self.products.create_index([("scraped_at", DESCENDING)])
            
            # Index unique sur SKU (évite les doublons)
            self.products.create_index([("sku", ASCENDING)], unique=True, sparse=True)
            
            # Index composé pour requêtes fréquentes
            self.products.create_index([
                ("category", ASCENDING),
                ("price", ASCENDING)
            ])
            
            # Index pour l'historique des prix
            self.price_history.create_index([
                ("sku", ASCENDING),
                ("date", DESCENDING)
            ])
            
            # Index pour les logs
            self.scraping_logs.create_index([("timestamp", DESCENDING)])
            self.scraping_logs.create_index([("status", ASCENDING)])
            
            logger.info("mongodb_indexes_created")
            
        except PyMongoError as e:
            logger.error("index_creation_failed", error=str(e))
    
    # ==================== PRODUITS - CRUD ====================
    
    def upsert_product(self, product: dict) -> Optional[str]:
        """
        Insère ou met à jour un produit.
        
        Utilise le titre + catégorie comme clé naturelle pour éviter
        les doublons.
        
        Args:
            product: Dictionnaire avec les données du produit
                Required: title, price
                Optional: description, rating, category, subcategory, etc.
            
        Returns:
            ID du document ou "updated" si mise à jour
        """
        try:
            # Ajouter les timestamps
            product["updated_at"] = datetime.utcnow()
            if "created_at" not in product:
                product["created_at"] = datetime.utcnow()
            
            # Upsert basé sur titre + catégorie
            result = self.products.update_one(
                {
                    "title": product.get("title"),
                    "category": product.get("category", "unknown")
                },
                {"$set": product},
                upsert=True
            )
            
            if result.upserted_id:
                logger.debug("product_inserted", title=product.get("title"))
                return str(result.upserted_id)
            
            logger.debug("product_updated", title=product.get("title"))
            return "updated"
            
        except PyMongoError as e:
            logger.error("product_upsert_failed", error=str(e))
            return None
    
    def bulk_upsert_products(self, products: list[dict]) -> dict:
        """
        Insère plusieurs produits en batch.
        
        Args:
            products: Liste de dictionnaires de produits
            
        Returns:
            {"inserted": N, "updated": M, "errors": E}
        """
        results = {"inserted": 0, "updated": 0, "errors": 0}
        
        for product in products:
            result = self.upsert_product(product)
            if result == "updated":
                results["updated"] += 1
            elif result:
                results["inserted"] += 1
            else:
                results["errors"] += 1
        
        logger.info("bulk_upsert_completed", **results)
        return results
    
    def find_products(
        self,
        query: dict = None,
        projection: dict = None,
        sort: list = None,
        limit: int = 100,
        skip: int = 0
    ) -> list[dict]:
        """
        Recherche des produits avec filtres.
        
        Args:
            query: Filtre MongoDB (ex: {"price": {"$lt": 500}})
            projection: Champs à retourner (ex: {"title": 1, "price": 1})
            sort: Critères de tri (ex: [("price", 1)])
            limit: Nombre max de résultats
            skip: Nombre de résultats à sauter (pagination)
            
        Returns:
            Liste de produits correspondants
            
        Example:
            >>> products = mongo.find_products(
            ...     query={"category": "laptops", "price": {"$lt": 1000}},
            ...     sort=[("price", 1)],
            ...     limit=10
            ... )
        """
        query = query or {}
        cursor = self.products.find(query, projection)
        
        if sort:
            cursor = cursor.sort(sort)
        
        return list(cursor.skip(skip).limit(limit))
    
    def search_products(self, text: str, limit: int = 20) -> list[dict]:
        """
        Recherche full-text dans les titres et descriptions.
        
        Args:
            text: Texte à rechercher
            limit: Nombre max de résultats
            
        Returns:
            Liste de produits triés par pertinence
        """
        return list(self.products.find(
            {"$text": {"$search": text}},
            {"score": {"$meta": "textScore"}}
        ).sort([("score", {"$meta": "textScore"})]).limit(limit))
    
    def get_product_by_id(self, product_id: str) -> Optional[dict]:
        """Récupère un produit par son ID MongoDB."""
        try:
            return self.products.find_one({"_id": ObjectId(product_id)})
        except:
            return None
    
    def get_product_by_sku(self, sku: str) -> Optional[dict]:
        """Récupère un produit par son SKU."""
        return self.products.find_one({"sku": sku})
    
    def delete_product(self, product_id: str) -> bool:
        """Supprime un produit."""
        try:
            result = self.products.delete_one({"_id": ObjectId(product_id)})
            return result.deleted_count > 0
        except:
            return False
    
    # ==================== REQUÊTES SPÉCIFIQUES ====================
    
    def get_products_by_category(self, category: str) -> list[dict]:
        """Récupère tous les produits d'une catégorie."""
        return self.find_products({"category": category})
    
    def get_products_by_subcategory(self, subcategory: str) -> list[dict]:
        """Récupère tous les produits d'une sous-catégorie."""
        return self.find_products({"subcategory": subcategory})
    
    def get_products_by_price_range(
        self,
        min_price: float,
        max_price: float
    ) -> list[dict]:
        """Récupère les produits dans une fourchette de prix."""
        return self.find_products({
            "price": {"$gte": min_price, "$lte": max_price}
        })
    
    def get_products_by_rating(self, min_rating: int) -> list[dict]:
        """Récupère les produits avec un rating minimum."""
        return self.find_products(
            {"rating": {"$gte": min_rating}},
            sort=[("rating", DESCENDING)]
        )
    
    def get_cheap_products(self, max_price: float, category: str = None) -> list[dict]:
        """Récupère les produits pas chers."""
        query = {"price": {"$lt": max_price}}
        if category:
            query["category"] = category
        return self.find_products(query, sort=[("price", ASCENDING)])
    
    def get_top_rated(self, limit: int = 10, category: str = None) -> list[dict]:
        """Récupère les produits les mieux notés."""
        query = {"category": category} if category else {}
        return self.find_products(
            query,
            sort=[("rating", DESCENDING), ("price", ASCENDING)],
            limit=limit
        )
    
    # ==================== HISTORIQUE DES PRIX ====================
    
    def record_price(self, sku: str, price: float) -> None:
        """
        Enregistre un point de prix pour le suivi temporel.
        
        Args:
            sku: Identifiant unique du produit
            price: Prix actuel
        """
        self.price_history.insert_one({
            "sku": sku,
            "price": price,
            "date": datetime.utcnow()
        })
    
    def get_price_history(self, sku: str, days: int = 30) -> list[dict]:
        """
        Récupère l'historique des prix d'un produit.
        
        Args:
            sku: Identifiant du produit
            days: Nombre de jours d'historique
            
        Returns:
            Liste de {price, date} triée par date
        """
        start_date = datetime.utcnow() - timedelta(days=days)
        
        return list(self.price_history.find(
            {"sku": sku, "date": {"$gte": start_date}},
            {"_id": 0, "price": 1, "date": 1}
        ).sort("date", ASCENDING))
    
    # ==================== AGRÉGATIONS ====================
    
    def get_stats(self) -> dict:
        """
        Calcule des statistiques globales du catalogue.
        
        Returns:
            {
                total_products: int,
                avg_price: float,
                min_price: float,
                max_price: float,
                avg_rating: float,
                categories: int
            }
        """
        pipeline = [
            {
                "$group": {
                    "_id": None,
                    "total_products": {"$sum": 1},
                    "avg_price": {"$avg": "$price"},
                    "min_price": {"$min": "$price"},
                    "max_price": {"$max": "$price"},
                    "avg_rating": {"$avg": "$rating"},
                    "categories": {"$addToSet": "$category"}
                }
            }
        ]
        
        result = list(self.products.aggregate(pipeline))
        
        if result:
            stats = result[0]
            stats["total_categories"] = len(stats.get("categories", []))
            del stats["_id"]
            del stats["categories"]
            return stats
        
        return {
            "total_products": 0,
            "avg_price": 0,
            "min_price": 0,
            "max_price": 0,
            "avg_rating": 0,
            "total_categories": 0
        }
    
    def get_stats_by_category(self) -> list[dict]:
        """
        Statistiques par catégorie.
        
        Returns:
            Liste de {category, count, avg_price, min_price, max_price, avg_rating}
        """
        pipeline = [
            {
                "$group": {
                    "_id": "$category",
                    "count": {"$sum": 1},
                    "avg_price": {"$avg": "$price"},
                    "min_price": {"$min": "$price"},
                    "max_price": {"$max": "$price"},
                    "avg_rating": {"$avg": "$rating"}
                }
            },
            {"$sort": {"count": -1}},
            {
                "$project": {
                    "_id": 0,
                    "category": "$_id",
                    "count": 1,
                    "avg_price": {"$round": ["$avg_price", 2]},
                    "min_price": 1,
                    "max_price": 1,
                    "avg_rating": {"$round": ["$avg_rating", 2]}
                }
            }
        ]
        
        return list(self.products.aggregate(pipeline))
    
    def get_stats_by_subcategory(self) -> list[dict]:
        """Statistiques par sous-catégorie."""
        pipeline = [
            {
                "$group": {
                    "_id": {"category": "$category", "subcategory": "$subcategory"},
                    "count": {"$sum": 1},
                    "avg_price": {"$avg": "$price"},
                    "avg_rating": {"$avg": "$rating"}
                }
            },
            {"$sort": {"_id.category": 1, "count": -1}},
            {
                "$project": {
                    "_id": 0,
                    "category": "$_id.category",
                    "subcategory": "$_id.subcategory",
                    "count": 1,
                    "avg_price": {"$round": ["$avg_price", 2]},
                    "avg_rating": {"$round": ["$avg_rating", 2]}
                }
            }
        ]
        
        return list(self.products.aggregate(pipeline))
    
    def get_price_distribution(self, buckets: list = None) -> list[dict]:
        """
        Distribution des prix par tranches.
        
        Args:
            buckets: Limites des tranches (ex: [0, 200, 500, 1000, 2000])
            
        Returns:
            Liste de {range, count, products}
        """
        buckets = buckets or [0, 100, 200, 500, 1000, 2000, 5000]
        
        pipeline = [
            {
                "$bucket": {
                    "groupBy": "$price",
                    "boundaries": buckets,
                    "default": "5000+",
                    "output": {
                        "count": {"$sum": 1},
                        "products": {"$push": "$title"},
                        "avg_rating": {"$avg": "$rating"}
                    }
                }
            }
        ]
        
        return list(self.products.aggregate(pipeline))
    
    def get_most_expensive_by_category(self) -> list[dict]:
        """Trouve le produit le plus cher de chaque catégorie."""
        pipeline = [
            {"$sort": {"price": -1}},
            {
                "$group": {
                    "_id": "$category",
                    "max_price": {"$first": "$price"},
                    "product_title": {"$first": "$title"},
                    "product_id": {"$first": "$_id"}
                }
            },
            {"$sort": {"max_price": -1}},
            {
                "$project": {
                    "_id": 0,
                    "category": "$_id",
                    "max_price": 1,
                    "product_title": 1
                }
            }
        ]
        
        return list(self.products.aggregate(pipeline))
    
    def get_value_ranking(self, limit: int = 10) -> list[dict]:
        """
        Classement par rapport qualité/prix.
        Score = rating / (price / 100)
        
        Args:
            limit: Nombre de produits à retourner
            
        Returns:
            Liste triée par score décroissant
        """
        pipeline = [
            {"$match": {"price": {"$gt": 0}, "rating": {"$gt": 0}}},
            {
                "$project": {
                    "title": 1,
                    "price": 1,
                    "rating": 1,
                    "category": 1,
                    "value_score": {
                        "$divide": [
                            "$rating",
                            {"$divide": ["$price", 100]}
                        ]
                    }
                }
            },
            {"$sort": {"value_score": -1}},
            {"$limit": limit}
        ]
        
        return list(self.products.aggregate(pipeline))
    
    def get_products_same_price(self) -> list[dict]:
        """Trouve les produits qui ont le même prix."""
        pipeline = [
            {
                "$group": {
                    "_id": "$price",
                    "count": {"$sum": 1},
                    "products": {"$push": {"title": "$title", "category": "$category"}}
                }
            },
            {"$match": {"count": {"$gt": 1}}},
            {"$sort": {"count": -1}},
            {
                "$project": {
                    "_id": 0,
                    "price": "$_id",
                    "count": 1,
                    "products": 1
                }
            }
        ]
        
        return list(self.products.aggregate(pipeline))
    
    # ==================== LOGS DE SCRAPING ====================
    
    def log_scraping_run(
        self,
        status: str,
        products_scraped: int,
        duration_seconds: float,
        images_stored: int = 0,
        errors: list = None
    ) -> None:
        """
        Enregistre un log de scraping.
        
        Args:
            status: "success" ou "failed"
            products_scraped: Nombre de produits scrapés
            duration_seconds: Durée d'exécution
            images_stored: Nombre d'images stockées
            errors: Liste des erreurs rencontrées
        """
        self.scraping_logs.insert_one({
            "timestamp": datetime.utcnow(),
            "status": status,
            "products_scraped": products_scraped,
            "images_stored": images_stored,
            "duration_seconds": round(duration_seconds, 2),
            "errors": errors or []
        })
        
        logger.info("scraping_logged", status=status, products=products_scraped)
    
    def get_scraping_history(self, limit: int = 10) -> list[dict]:
        """Récupère l'historique des runs de scraping."""
        return list(
            self.scraping_logs.find({}, {"_id": 0})
            .sort("timestamp", DESCENDING)
            .limit(limit)
        )
    
    # ==================== UTILITAIRES ====================
    
    def count_products(self, query: dict = None) -> int:
        """Compte le nombre de produits."""
        return self.products.count_documents(query or {})
    
    def get_categories(self) -> list[str]:
        """Liste toutes les catégories uniques."""
        return self.products.distinct("category")
    
    def get_subcategories(self, category: str = None) -> list[str]:
        """Liste les sous-catégories."""
        query = {"category": category} if category else {}
        return self.products.distinct("subcategory", query)
    
    def get_all_data(self) -> dict:
        """
        Exporte toutes les données.
        
        Returns:
            {products: [...], exported_at: datetime}
        """
        products = list(self.products.find({}, {"_id": 0}))
        
        return {
            "products": products,
            "count": len(products),
            "exported_at": datetime.utcnow().isoformat()
        }
    
    def delete_all_products(self) -> int:
        """
        Supprime tous les produits (reset).
        
        ⚠️ ATTENTION: Opération destructive!
        
        Returns:
            Nombre de produits supprimés
        """
        result = self.products.delete_many({})
        logger.warning("all_products_deleted", count=result.deleted_count)
        return result.deleted_count
    
    def close(self) -> None:
        """Ferme la connexion MongoDB."""
        self.client.close()
        logger.info("mongodb_connection_closed")


# Test du module
if __name__ == "__main__":
    print("Test du client MongoDB...")
    
    mongo = MongoDBStorage()
    
    # Test insertion
    test_product = {
        "title": "Test Laptop",
        "price": 999.99,
        "rating": 4,
        "category": "computers",
        "subcategory": "laptops",
        "description": "A test laptop"
    }
    
    result = mongo.upsert_product(test_product)
    print(f"Insert result: {result}")
    
    # Test recherche
    products = mongo.find_products({"category": "computers"})
    print(f"Products found: {len(products)}")
    
    # Test stats
    stats = mongo.get_stats()
    print(f"Stats: {stats}")
    
    # Cleanup
    mongo.close()
    print("Tests terminés!")
