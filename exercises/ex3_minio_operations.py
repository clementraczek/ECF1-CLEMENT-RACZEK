"""
Exercice 3 : Opérations MinIO - SOLUTION COMPLÈTE

Ce fichier contient les solutions pour l'exercice 3
sur la manipulation du stockage objet MinIO.

Usage:
    python exercises/ex3_minio_operations.py
"""

import sys
import os
import io
import json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from PIL import Image
from src.storage import MinIOStorage, MongoDBStorage
from config.settings import minio_config


def exercise_3():
    """
    Solutions de l'exercice 3 sur les opérations MinIO.
    """
    minio = MinIOStorage()
    mongo = MongoDBStorage()
    
    print("="*60)
    print("EXERCICE 3 : OPÉRATIONS MINIO - SOLUTIONS")
    print("="*60)
    
    # ============================================================
    # 3.1 Listez toutes les images et calculez la taille totale
    # ============================================================
    print("\n3.1 Liste des images et taille totale")
    print("-" * 40)
    
    images = minio.list_images()
    total_images = len(images)
    total_size_kb = sum(img['size'] for img in images) / 1024
    
    print(f"Code:")
    print(f"  images = minio.list_images()")
    print(f"  total_images = len(images)")
    print(f"  total_size_kb = sum(img['size'] for img in images) / 1024")
    print(f"\nRésultat:")
    print(f"  - Nombre d'images: {total_images}")
    print(f"  - Taille totale: {total_size_kb:.2f} KB ({total_size_kb/1024:.2f} MB)")
    
    # Afficher quelques images
    print(f"\nPremières images:")
    for img in images[:5]:
        print(f"  - {img['name']}: {img['size']/1024:.1f} KB")
    
    # ============================================================
    # 3.2 Créez des thumbnails (100x100)
    # ============================================================
    print("\n3.2 Création de thumbnails")
    print("-" * 40)
    
    thumbnails_created = 0
    thumbnail_errors = []
    
    print("Code:")
    print("""  for img in images:
      # Télécharger l'image
      image_data = minio.get_image(img['name'])
      
      # Créer le thumbnail avec PIL
      image = Image.open(io.BytesIO(image_data))
      image.thumbnail((100, 100))
      
      # Sauvegarder
      buffer = io.BytesIO()
      image.save(buffer, format='JPEG')
      
      # Upload avec préfixe 'thumbnails/'
      thumbnail_name = f"thumbnails/{img['name']}"
      minio.upload_image(buffer.getvalue(), thumbnail_name)
  """)
    
    # Créer les thumbnails
    for img in images[:10]:  # Limiter à 10 pour le test
        try:
            # Télécharger l'image originale
            image_data = minio.get_image(img['name'])
            
            if image_data:
                # Ouvrir avec PIL
                image = Image.open(io.BytesIO(image_data))
                
                # Créer le thumbnail
                image.thumbnail((100, 100))
                
                # Convertir en bytes
                buffer = io.BytesIO()
                # Convertir en RGB si nécessaire (pour les PNG avec transparence)
                if image.mode in ('RGBA', 'P'):
                    image = image.convert('RGB')
                image.save(buffer, format='JPEG', quality=85)
                
                # Upload
                thumbnail_name = f"thumbnails/{img['name']}"
                result = minio.upload_image(buffer.getvalue(), thumbnail_name)
                
                if result:
                    thumbnails_created += 1
                    
        except Exception as e:
            thumbnail_errors.append(f"{img['name']}: {str(e)}")
    
    print(f"\nRésultat:")
    print(f"  - Thumbnails créés: {thumbnails_created}")
    print(f"  - Erreurs: {len(thumbnail_errors)}")
    
    # ============================================================
    # 3.3 URL présignée pour le produit le plus cher
    # ============================================================
    print("\n3.3 URL présignée pour le produit le plus cher")
    print("-" * 40)
    
    presigned_url = None
    
    # Trouver le produit le plus cher
    most_expensive = mongo.find_products(
        query={},
        sort=[("price", -1)],
        limit=1
    )
    
    if most_expensive:
        product = most_expensive[0]
        print(f"Produit le plus cher: {product.get('title', 'N/A')[:40]}...")
        print(f"Prix: ${product.get('price', 0)}")
        
        # Trouver l'image correspondante
        minio_ref = product.get('minio_image_ref', '')
        
        if minio_ref:
            # Extraire le nom de l'objet depuis l'URI minio://bucket/path
            object_name = minio_ref.replace(f"minio://{minio_config.bucket_images}/", "")
            
            # Générer l'URL présignée (24h)
            presigned_url = minio.get_presigned_url(object_name, expires_hours=24)
            
            print(f"\nURL présignée (24h):")
            if presigned_url:
                print(f"  {presigned_url[:80]}...")
            else:
                print("  [Non disponible - image non trouvée]")
        else:
            print("\n  [Pas de référence image pour ce produit]")
    else:
        print("  [Aucun produit trouvé]")
    
    print("\nCode:")
    print("""  presigned_url = minio.get_presigned_url(
      object_name,
      expires_hours=24
  )""")
    
    # ============================================================
    # 3.4 Rapport JSON des stats par catégorie
    # ============================================================
    print("\n3.4 Rapport JSON des stats par catégorie d'images")
    print("-" * 40)
    
    # Calculer les stats par catégorie
    stats_report = minio.get_images_by_category()
    
    print("Code:")
    print("""  stats_report = {}
  for img in images:
      category = img['name'].split('/')[0]
      if category not in stats_report:
          stats_report[category] = {"count": 0, "size_kb": 0}
      stats_report[category]["count"] += 1
      stats_report[category]["size_kb"] += img['size'] / 1024
  """)
    
    print(f"\nStats par catégorie:")
    for cat, stats in stats_report.items():
        print(f"  - {cat}: {stats['count']} images, {stats['size_kb']:.2f} KB")
    
    # Upload du rapport
    report_data = {
        "generated_at": datetime.utcnow().isoformat(),
        "total_images": total_images,
        "total_size_kb": total_size_kb,
        "by_category": stats_report
    }
    
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    report_uri = minio.upload_json(report_data, f"image_stats_report_{timestamp}.json")
    
    print(f"\nRapport uploadé: {report_uri}")
    
    # ============================================================
    # 3.5 Backup de toutes les images
    # ============================================================
    print("\n3.5 Backup des images vers un nouveau bucket")
    print("-" * 40)
    
    backup_created = False
    backup_count = 0
    
    # Créer le bucket de backup
    backup_date = datetime.utcnow().strftime("%Y%m%d")
    backup_bucket = f"backup-{backup_date}"
    
    print(f"Code:")
    print(f"""  # Créer le bucket de backup
  backup_bucket = f"backup-{{date}}"
  minio.create_backup_bucket(backup_bucket)
  
  # Copier chaque image
  for img in images:
      minio.copy_to_bucket(
          source_bucket="{minio_config.bucket_images}",
          source_object=img['name'],
          dest_bucket=backup_bucket,
          dest_object=img['name']
      )
  """)
    
    # Créer le bucket
    if minio.create_backup_bucket(backup_bucket):
        print(f"\nBucket créé: {backup_bucket}")
        
        # Copier les images (limiter à 5 pour le test)
        for img in images[:5]:
            try:
                success = minio.copy_to_bucket(
                    source_bucket=minio_config.bucket_images,
                    source_object=img['name'],
                    dest_bucket=backup_bucket,
                    dest_object=img['name']
                )
                if success:
                    backup_count += 1
            except Exception as e:
                print(f"  Erreur copie {img['name']}: {e}")
        
        backup_created = True
        print(f"Images copiées: {backup_count}")
    else:
        print("  Erreur création du bucket de backup")
    
    # Fermer les connexions
    mongo.close()
    
    print("\n" + "="*60)
    print("FIN DE L'EXERCICE 3")
    print("="*60)
    
    return {
        "total_images": total_images,
        "total_size_kb": round(total_size_kb, 2),
        "thumbnails_created": thumbnails_created,
        "presigned_url": presigned_url[:50] + "..." if presigned_url else None,
        "stats_report": stats_report,
        "backup_created": backup_created,
        "backup_count": backup_count
    }


if __name__ == "__main__":
    results = exercise_3()
    
    print("\n" + "="*60)
    print("RÉSUMÉ")
    print("="*60)
    for key, value in results.items():
        print(f"  {key}: {value}")
