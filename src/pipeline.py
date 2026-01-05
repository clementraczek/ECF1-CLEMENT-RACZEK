"""
Pipeline de données e-commerce.

Ce module orchestre le flux complet de données :
1. EXTRACT : Scraping des produits depuis webscraper.io
2. TRANSFORM : Nettoyage et enrichissement des données
3. LOAD : Stockage dans MongoDB (métadonnées) et MinIO (images)

Usage:
    # En ligne de commande
    python -m src.pipeline --pages 3 --export-csv
    
    # En Python
    >>> pipeline = EcommercePipeline()
    >>> stats = pipeline.run(max_pages=5)
    >>> print(stats)
"""

from datetime import datetime
from typing import Optional
import pandas as pd
from tqdm import tqdm
import structlog

# Imports locaux
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.ingestion import EcommerceScraper, Product
from src.storage import MinIOStorage, MongoDBStorage

logger = structlog.get_logger()


class EcommercePipeline:
    """
    Pipeline ETL complet pour les données e-commerce.
    
    Ce pipeline :
    - Scrape les produits depuis webscraper.io
    - Télécharge et stocke les images dans MinIO
    - Stocke les métadonnées dans MongoDB
    - Génère des exports (CSV, JSON, Parquet)
    - Calcule des analytics
    
    Attributes:
        scraper: Instance du scraper
        minio: Client MinIO
        mongodb: Client MongoDB
        stats: Statistiques d'exécution
    
    Example:
        >>> pipeline = EcommercePipeline()
        >>> stats = pipeline.run(max_pages=5, download_images=True)
        >>> print(f"Produits: {stats['products_scraped']}")
        >>> pipeline.close()
    """
    
    def __init__(self):
        """Initialise le pipeline avec tous les composants."""
        self.scraper = EcommerceScraper()
        self.minio = MinIOStorage()
        self.mongodb = MongoDBStorage()
        
        # Statistiques d'exécution
        self.stats = {
            "products_scraped": 0,
            "images_stored": 0,
            "errors": []
        }
    
    def _generate_image_path(self, product: Product) -> str:
        """
        Génère le chemin de stockage de l'image.
        
        Format: {subcategory}/{sku}.jpg
        
        Args:
            product: Produit
            
        Returns:
            Chemin dans le bucket MinIO
        """
        category = product.subcategory or product.category or "other"
        return f"{category}/{product.sku}.jpg"
    
    def process_product(
        self,
        product: Product,
        download_image: bool = True
    ) -> Optional[dict]:
        """
        Traite un produit complet : image + métadonnées.
        
        Args:
            product: Produit à traiter
            download_image: Télécharger l'image
            
        Returns:
            Dictionnaire du produit traité ou None si erreur
        """
        try:
            # Télécharger l'image si demandé
            minio_ref = None
            
            if download_image and product.image_url:
                product = self.scraper.download_image(product)
                
                if product.image_data:
                    image_path = self._generate_image_path(product)
                    minio_ref = self.minio.upload_image(
                        product.image_data,
                        image_path
                    )
                    
                    if minio_ref:
                        self.stats["images_stored"] += 1
            
            # Préparer les données pour MongoDB
            product_data = product.to_dict()
            product_data["minio_image_ref"] = minio_ref
            
            # Stocker dans MongoDB
            self.mongodb.upsert_product(product_data)
            
            # Enregistrer le prix pour l'historique
            self.mongodb.record_price(product.sku, product.price)
            
            self.stats["products_scraped"] += 1
            
            return product_data
            
        except Exception as e:
            error_msg = f"Error processing {product.title}: {str(e)}"
            logger.error("product_processing_failed", error=error_msg)
            self.stats["errors"].append(error_msg)
            return None
    
    def run(
        self,
        categories: list[str] = None,
        max_pages: int = 5,
        download_images: bool = True,
        show_progress: bool = True
    ) -> dict:
        """
        Exécute le pipeline complet.
        
        Args:
            categories: Sous-catégories à scraper (None = toutes)
                       Ex: ["laptops", "tablets"]
            max_pages: Nombre max de pages par catégorie
            download_images: Télécharger les images
            show_progress: Afficher une barre de progression
            
        Returns:
            Statistiques d'exécution
            
        Example:
            >>> stats = pipeline.run(
            ...     categories=["laptops"],
            ...     max_pages=2,
            ...     download_images=True
            ... )
        """
        start_time = datetime.utcnow()
        logger.info("pipeline_started", 
                   categories=categories, 
                   max_pages=max_pages,
                   download_images=download_images)
        
        try:
            # Récupérer les URLs de catégories
            urls = self.scraper.get_all_category_urls()
            
            # Filtrer si catégories spécifiées
            if categories:
                urls = [
                    (cat, subcat, url)
                    for cat, subcat, url in urls
                    if subcat in categories
                ]
            
            # Scraper chaque catégorie
            for category, subcategory, url in urls:
                logger.info("processing_category", 
                           category=category, 
                           subcategory=subcategory)
                
                # Collecter les produits
                products = list(self.scraper.scrape_category(
                    url=url,
                    category=category,
                    subcategory=subcategory,
                    max_pages=max_pages
                ))
                
                # Traiter avec barre de progression
                if show_progress:
                    iterator = tqdm(
                        products,
                        desc=f"Processing {subcategory}",
                        unit="product"
                    )
                else:
                    iterator = products
                
                for product in iterator:
                    self.process_product(product, download_images)
            
            # Log de succès
            duration = (datetime.utcnow() - start_time).total_seconds()
            self.mongodb.log_scraping_run(
                status="success",
                products_scraped=self.stats["products_scraped"],
                images_stored=self.stats["images_stored"],
                duration_seconds=duration,
                errors=self.stats["errors"]
            )
            
        except Exception as e:
            logger.error("pipeline_failed", error=str(e))
            
            duration = (datetime.utcnow() - start_time).total_seconds()
            self.mongodb.log_scraping_run(
                status="failed",
                products_scraped=self.stats["products_scraped"],
                images_stored=self.stats["images_stored"],
                duration_seconds=duration,
                errors=[str(e)] + self.stats["errors"]
            )
        
        finally:
            # Calculer les stats finales
            end_time = datetime.utcnow()
            self.stats["duration_seconds"] = (end_time - start_time).total_seconds()
            self.stats["start_time"] = start_time.isoformat()
            self.stats["end_time"] = end_time.isoformat()
        
        logger.info("pipeline_completed", **self.stats)
        return self.stats
    
    # ==================== EXPORTS ====================
    
    def export_csv(self, filepath: str = None) -> Optional[str]:
        """
        Exporte les produits en CSV.
        
        Args:
            filepath: Chemin local (optionnel)
            
        Returns:
            URI MinIO du fichier exporté
        """
        products = self.mongodb.find_products(limit=10000)
        
        if not products:
            logger.warning("no_products_to_export")
            return None
        
        # Convertir en DataFrame
        df = pd.DataFrame(products)
        
        # Nettoyer les colonnes MongoDB
        if "_id" in df.columns:
            df["_id"] = df["_id"].astype(str)
        
        # Convertir les dates
        for col in ["created_at", "updated_at", "scraped_at"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col]).dt.strftime("%Y-%m-%d %H:%M:%S")
        
        # Sauvegarder localement si demandé
        if filepath:
            df.to_csv(filepath, index=False)
            logger.info("csv_exported_locally", path=filepath)
        
        # Upload vers MinIO
        csv_content = df.to_csv(index=False)
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        
        uri = self.minio.upload_csv(csv_content, f"products_export_{timestamp}.csv")
        logger.info("csv_exported_to_minio", uri=uri)
        
        return uri
    
    def export_json(self, filepath: str = None) -> Optional[str]:
        """
        Exporte toutes les données en JSON.
        
        Args:
            filepath: Chemin local (optionnel)
            
        Returns:
            URI MinIO
        """
        import json
        
        data = self.mongodb.get_all_data()
        
        if filepath:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        return self.minio.upload_json(data, f"full_export_{timestamp}.json")
    
    def export_parquet(self, filepath: str = None) -> Optional[str]:
        """
        Exporte les données en Parquet (format optimisé pour l'analyse).
        
        Args:
            filepath: Chemin local (optionnel)
            
        Returns:
            URI MinIO
        """
        import io
        
        products = self.mongodb.find_products(limit=10000)
        
        if not products:
            return None
        
        df = pd.DataFrame(products)
        
        # Nettoyer
        if "_id" in df.columns:
            df["_id"] = df["_id"].astype(str)
        
        # Sauvegarder localement
        if filepath:
            df.to_parquet(filepath, index=False)
        
        # Upload vers MinIO
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        
        return self.minio.upload_export(
            buffer.getvalue(),
            f"products_export_{timestamp}.parquet",
            "application/octet-stream"
        )
    
    def create_backup(self) -> Optional[str]:
        """
        Crée une sauvegarde complète.
        
        Returns:
            URI MinIO du backup
        """
        data = self.mongodb.get_all_data()
        return self.minio.create_backup(data, "ecommerce_backup")
    
    # ==================== ANALYTICS ====================
    
    def get_analytics(self) -> dict:
        """
        Génère un rapport d'analytics complet.
        
        Returns:
            Dictionnaire avec toutes les statistiques
        """
        return {
            "catalog_stats": self.mongodb.get_stats(),
            "by_category": self.mongodb.get_stats_by_category(),
            "by_subcategory": self.mongodb.get_stats_by_subcategory(),
            "price_distribution": self.mongodb.get_price_distribution(),
            "most_expensive": self.mongodb.get_most_expensive_by_category(),
            "value_ranking": self.mongodb.get_value_ranking(10),
            "storage_stats": self.minio.get_stats(),
            "scraping_history": self.mongodb.get_scraping_history(5)
        }
    
    def print_analytics(self) -> None:
        """Affiche les analytics de manière formatée."""
        analytics = self.get_analytics()
        
        print("\n" + "="*60)
        print("RAPPORT D'ANALYTICS E-COMMERCE")
        print("="*60)
        
        # Stats globales
        stats = analytics.get("catalog_stats", {})
        print(f"\n📊 STATISTIQUES GLOBALES")
        print(f"   Total produits : {stats.get('total_products', 0)}")
        print(f"   Prix moyen     : ${stats.get('avg_price', 0):.2f}")
        print(f"   Prix min       : ${stats.get('min_price', 0):.2f}")
        print(f"   Prix max       : ${stats.get('max_price', 0):.2f}")
        print(f"   Rating moyen   : {stats.get('avg_rating', 0):.1f}★")
        print(f"   Catégories     : {stats.get('total_categories', 0)}")
        
        # Par catégorie
        print(f"\n📁 PAR CATÉGORIE")
        for cat in analytics.get("by_category", []):
            print(f"   {cat['category']}: {cat['count']} produits, "
                  f"${cat['avg_price']:.2f} moy, {cat['avg_rating']:.1f}★")
        
        # Top rapport qualité/prix
        print(f"\n🏆 TOP 5 RAPPORT QUALITÉ/PRIX")
        for i, p in enumerate(analytics.get("value_ranking", [])[:5], 1):
            print(f"   {i}. {p['title'][:40]}... "
                  f"(${p['price']:.2f}, {p['rating']}★)")
        
        # Stockage
        storage = analytics.get("storage_stats", {})
        print(f"\n💾 STOCKAGE")
        for bucket, info in storage.items():
            print(f"   {bucket}: {info['count']} fichiers, "
                  f"{info['total_size_mb']:.2f} MB")
        
        print("\n" + "="*60)
    
    def close(self) -> None:
        """Ferme toutes les connexions."""
        self.scraper.close()
        self.mongodb.close()
        logger.info("pipeline_closed")


def main():
    """Point d'entrée en ligne de commande."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Pipeline de scraping e-commerce",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
  python -m src.pipeline --pages 3
  python -m src.pipeline --categories laptops tablets --pages 5
  python -m src.pipeline --pages 2 --export-csv --export-json
  python -m src.pipeline --no-images --pages 10
        """
    )
    
    parser.add_argument(
        "--pages", 
        type=int, 
        default=3,
        help="Nombre de pages à scraper par catégorie (défaut: 3)"
    )
    parser.add_argument(
        "--categories",
        nargs="+",
        choices=["laptops", "tablets", "touch"],
        help="Catégories à scraper (défaut: toutes)"
    )
    parser.add_argument(
        "--no-images",
        action="store_true",
        help="Ne pas télécharger les images"
    )
    parser.add_argument(
        "--export-csv",
        action="store_true",
        help="Exporter en CSV après le scraping"
    )
    parser.add_argument(
        "--export-json",
        action="store_true",
        help="Exporter en JSON après le scraping"
    )
    parser.add_argument(
        "--export-parquet",
        action="store_true",
        help="Exporter en Parquet après le scraping"
    )
    parser.add_argument(
        "--backup",
        action="store_true",
        help="Créer un backup après le scraping"
    )
    parser.add_argument(
        "--analytics",
        action="store_true",
        help="Afficher les analytics après le scraping"
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Mode silencieux (pas de barre de progression)"
    )
    
    args = parser.parse_args()
    
    # Créer et exécuter le pipeline
    pipeline = EcommercePipeline()
    
    try:
        # Exécuter le scraping
        print("\n🚀 Démarrage du pipeline de scraping...")
        
        stats = pipeline.run(
            categories=args.categories,
            max_pages=args.pages,
            download_images=not args.no_images,
            show_progress=not args.quiet
        )
        
        # Afficher les résultats
        print("\n" + "="*50)
        print("✅ PIPELINE TERMINÉ")
        print("="*50)
        print(f"   Produits scrapés : {stats['products_scraped']}")
        print(f"   Images stockées  : {stats['images_stored']}")
        print(f"   Durée            : {stats['duration_seconds']:.2f}s")
        print(f"   Erreurs          : {len(stats['errors'])}")
        
        if stats['errors']:
            print("\n⚠️  Erreurs rencontrées:")
            for error in stats['errors'][:5]:
                print(f"   - {error}")
        
        # Exports
        if args.export_csv:
            print("\n📄 Export CSV...")
            uri = pipeline.export_csv()
            print(f"   → {uri}")
        
        if args.export_json:
            print("\n📄 Export JSON...")
            uri = pipeline.export_json()
            print(f"   → {uri}")
        
        if args.export_parquet:
            print("\n📄 Export Parquet...")
            uri = pipeline.export_parquet()
            print(f"   → {uri}")
        
        if args.backup:
            print("\n💾 Création du backup...")
            uri = pipeline.create_backup()
            print(f"   → {uri}")
        
        # Analytics
        if args.analytics:
            pipeline.print_analytics()
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Interruption par l'utilisateur")
    
    except Exception as e:
        print(f"\n❌ Erreur: {str(e)}")
        raise
    
    finally:
        pipeline.close()
        print("\n👋 Pipeline fermé.")


if __name__ == "__main__":
    main()
