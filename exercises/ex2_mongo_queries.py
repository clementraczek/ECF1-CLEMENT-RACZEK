"""
Exercice 2 : Requêtes MongoDB - SOLUTION COMPLÈTE

Ce fichier contient les solutions pour l'exercice 2
sur les requêtes et agrégations MongoDB.

Usage:
    python exercises/ex2_mongo_queries.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.storage import MongoDBStorage


def exercise_2():
    """
    Solutions de l'exercice 2 sur les requêtes MongoDB.
    """
    mongo = MongoDBStorage()
    
    print("="*60)
    print("EXERCICE 2 : REQUÊTES MONGODB - SOLUTIONS")
    print("="*60)
    
    # ============================================================
    # 2.1 Trouvez tous les laptops avec un prix < 500$
    # ============================================================
    print("\n2.1 Laptops à moins de 500$")
    print("-" * 40)
    
    cheap_laptops = mongo.find_products({
        "subcategory": "laptops",
        "price": {"$lt": 500}
    })
    
    print(f"Requête: {{'subcategory': 'laptops', 'price': {{'$lt': 500}}}}")
    print(f"Résultat: {len(cheap_laptops)} produits trouvés")
    
    for p in cheap_laptops[:3]:
        print(f"  - {p.get('title', 'N/A')[:40]}... : ${p.get('price', 0)}")
    
    # ============================================================
    # 2.2 Trouvez le produit le plus cher de chaque catégorie
    # ============================================================
    print("\n2.2 Produit le plus cher par catégorie")
    print("-" * 40)
    
    most_expensive_by_cat = list(mongo.products.aggregate([
        {"$sort": {"price": -1}},
        {
            "$group": {
                "_id": "$category",
                "max_price": {"$first": "$price"},
                "product_title": {"$first": "$title"},
                "product_rating": {"$first": "$rating"}
            }
        },
        {"$sort": {"max_price": -1}},
        {
            "$project": {
                "_id": 0,
                "category": "$_id",
                "max_price": 1,
                "product_title": 1,
                "product_rating": 1
            }
        }
    ]))
    
    print("Pipeline d'agrégation:")
    print("  1. $sort: {price: -1}")
    print("  2. $group: {_id: '$category', max_price: {$first: '$price'}, ...}")
    print("  3. $sort: {max_price: -1}")
    print(f"\nRésultat: {len(most_expensive_by_cat)} catégories")
    
    for item in most_expensive_by_cat:
        title = item.get('product_title', 'N/A')[:30]
        print(f"  - {item.get('category', 'N/A')}: {title}... (${item.get('max_price', 0)})")
    
    # ============================================================
    # 2.3 Calculez le prix moyen des produits avec rating >= 4
    # ============================================================
    print("\n2.3 Prix moyen des produits bien notés (rating >= 4)")
    print("-" * 40)
    
    avg_price_pipeline = list(mongo.products.aggregate([
        {"$match": {"rating": {"$gte": 4}}},
        {
            "$group": {
                "_id": None,
                "avg_price": {"$avg": "$price"},
                "count": {"$sum": 1},
                "min_price": {"$min": "$price"},
                "max_price": {"$max": "$price"}
            }
        }
    ]))
    
    avg_price_good_rating = avg_price_pipeline[0] if avg_price_pipeline else {"avg_price": 0, "count": 0}
    
    print("Pipeline:")
    print("  1. $match: {rating: {$gte: 4}}")
    print("  2. $group: {_id: null, avg_price: {$avg: '$price'}, ...}")
    print(f"\nRésultat:")
    print(f"  - Produits avec rating >= 4: {avg_price_good_rating.get('count', 0)}")
    print(f"  - Prix moyen: ${avg_price_good_rating.get('avg_price', 0):.2f}")
    
    # ============================================================
    # 2.4 Trouvez les produits Samsung ou Apple
    # ============================================================
    print("\n2.4 Produits Samsung ou Apple")
    print("-" * 40)
    
    brand_products = mongo.find_products({
        "$or": [
            {"title": {"$regex": "Samsung", "$options": "i"}},
            {"title": {"$regex": "Apple", "$options": "i"}}
        ]
    })
    
    print("Requête avec $or et $regex:")
    print(f"Résultat: {len(brand_products)} produits")
    
    for p in brand_products[:5]:
        print(f"  - {p.get('title', 'N/A')[:50]}...")
    
    # ============================================================
    # 2.5 Classement par rapport qualité/prix
    # ============================================================
    print("\n2.5 Top 10 rapport qualité/prix")
    print("-" * 40)
    
    value_ranking = list(mongo.products.aggregate([
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
        {"$limit": 10}
    ]))
    
    print("Formule: value_score = rating / (price / 100)")
    print(f"\nTop 10:")
    
    for i, p in enumerate(value_ranking, 1):
        title = p.get('title', 'N/A')[:35]
        print(f"  {i}. {title}... "
              f"(${p.get('price', 0)}, {p.get('rating', 0)}★, score: {p.get('value_score', 0):.2f})")
    
    # ============================================================
    # 2.6 Distribution par tranche de prix
    # ============================================================
    print("\n2.6 Distribution par tranche de prix")
    print("-" * 40)
    
    price_ranges = list(mongo.products.aggregate([
        {
            "$bucket": {
                "groupBy": "$price",
                "boundaries": [0, 200, 500, 1000, 2000],
                "default": "2000+",
                "output": {
                    "count": {"$sum": 1},
                    "avg_rating": {"$avg": "$rating"}
                }
            }
        }
    ]))
    
    print("Tranches: 0-200, 200-500, 500-1000, 1000-2000, 2000+")
    print(f"\nRésultat:")
    
    for bucket in price_ranges:
        range_label = f"${bucket['_id']}" if isinstance(bucket['_id'], int) else bucket['_id']
        print(f"  - {range_label}: {bucket['count']} produits, "
              f"rating moy: {bucket.get('avg_rating', 0):.1f}★")
    
    # ============================================================
    # 2.7 Produits avec le même prix (doublons)
    # ============================================================
    print("\n2.7 Produits avec le même prix")
    print("-" * 40)
    
    same_price_products = list(mongo.products.aggregate([
        {
            "$group": {
                "_id": "$price",
                "count": {"$sum": 1},
                "products": {
                    "$push": {
                        "title": "$title",
                        "category": "$category"
                    }
                }
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
    ]))
    
    print(f"Résultat: {len(same_price_products)} prix en doublon")
    
    for item in same_price_products[:3]:
        print(f"\n  Prix ${item['price']} ({item['count']} produits):")
        for p in item['products'][:3]:
            print(f"    - {p.get('title', 'N/A')[:40]}...")
    
    mongo.close()
    
    print("\n" + "="*60)
    print("FIN DE L'EXERCICE 2")
    print("="*60)
    
    return {
        "cheap_laptops": cheap_laptops,
        "most_expensive_by_cat": most_expensive_by_cat,
        "avg_price_good_rating": avg_price_good_rating,
        "brand_products": brand_products,
        "value_ranking": value_ranking,
        "price_ranges": price_ranges,
        "same_price_products": same_price_products
    }


if __name__ == "__main__":
    exercise_2()
