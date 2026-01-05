import pandas as pd
import io
from datetime import datetime
from src.storage.minio_client import MinioClient
from config.settings import minio_config

class BooksCleaner:
    def __init__(self):
        self.storage = MinioClient()
        self.bucket_bronze = minio_config.bucket_bronze
        self.bucket_silver = minio_config.bucket_silver
        # Dossier source défini dans ton Spider Scrapy
        self.source_prefix = "scraping/books/"

    def clean_data(self, df):
        """Nettoyage des données de livres scrapées"""
        # 1. Suppression des doublons (basé sur le titre pour ce site)
        df = df.drop_duplicates(subset=['title'])

        # 2. Gestion des prix (déjà en float via Scrapy, mais on sécurise)
        df['price_gbp'] = pd.to_numeric(df['price_gbp'], errors='coerce')

        # 3. Normalisation de la disponibilité
        # 'In stock (19 available)' -> 'In Stock'
        if 'availability' in df.columns:
            df['availability'] = df['availability'].apply(
                lambda x: 'In Stock' if 'stock' in str(x).lower() else 'Out of Stock'
            )

        # 4. Ajout des métadonnées de traitement
        df['cleaned_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        return df

    def run(self):
        print(f"🔍 Scan du dossier '{self.source_prefix}' dans {self.bucket_bronze}...")
        
        try:
            # Liste les objets dans le dossier scraping/books/
            objects = self.storage.client.list_objects(
                self.bucket_bronze, 
                prefix=self.source_prefix, 
                recursive=True
            )
            
            files = [obj.object_name for obj in objects if obj.object_name.endswith('.csv')]

            if not files:
                print(f"❌ Aucun fichier trouvé dans {self.source_prefix}")
                return

            # Sélection du dernier crawl
            latest_file = sorted(files)[-1]
            print(f"📂 Fichier détecté : {latest_file}")

            # Lecture
            response = self.storage.client.get_object(self.bucket_bronze, latest_file)
            df = pd.read_csv(io.BytesIO(response.read()))
            
            print(f"🧹 Nettoyage de {len(df)} livres...")
            df_cleaned = self.clean_data(df)

            # Export vers Silver
            silver_filename = latest_file.split('/')[-1].replace('.csv', '_cleaned.csv')
            silver_path = f"books/{silver_filename}"
            
            csv_buffer = df_cleaned.to_csv(index=False).encode('utf-8')
            
            self.storage.client.put_object(
                bucket_name=self.bucket_silver,
                object_name=silver_path,
                data=io.BytesIO(csv_buffer),
                length=len(csv_buffer),
                content_type="text/csv"
            )
            print(f"✅ [SILVER] Données livres prêtes : {silver_path}")

        except Exception as e:
            print(f"❌ Erreur : {str(e)}")

if __name__ == "__main__":
    BooksCleaner().run()