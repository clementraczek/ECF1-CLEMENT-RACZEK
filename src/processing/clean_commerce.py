import pandas as pd
import io
from datetime import datetime
from src.storage.minio_client import MinioClient
from config.settings import minio_config

class EcommerceCleaner:
    def __init__(self):
        self.storage = MinioClient()
        self.bucket_bronze = minio_config.bucket_bronze
        self.bucket_silver = minio_config.bucket_silver
        # Dossier source spécifique
        self.source_prefix = "scraping/ecommerce/"

    def clean_data(self, df):
        """Nettoyage des données e-commerce"""
        # 1. Suppression des doublons basés sur le SKU
        df = df.drop_duplicates(subset=['sku'])

        # 2. Conversion du prix en numérique (remplace les erreurs par NaN)
        df['price'] = pd.to_numeric(df['price'], errors='coerce')

        # 3. Nettoyage des descriptions (retrait des sauts de ligne)
        if 'description' in df.columns:
            df['description'] = df['description'].str.replace(r'[\r\n]+', ' ', regex=True).str.strip()

        # 4. Traçabilité
        df['imported_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        return df

    def run(self):
        print(f"🔍 Scan du dossier '{self.source_prefix}' dans le bucket {self.bucket_bronze}...")
        
        try:
            # Liste tous les fichiers commençant par le préfixe scraping/ecommerce/
            objects = self.storage.client.list_objects(
                self.bucket_bronze, 
                prefix=self.source_prefix, 
                recursive=True
            )
            
            # Filtrer pour ne garder que les fichiers CSV
            files = [obj.object_name for obj in objects if obj.object_name.endswith('.csv')]

            if not files:
                print(f"❌ Aucun fichier trouvé dans {self.source_prefix}")
                return

            # Trier par nom (souvent le timestamp est dans le nom) pour prendre le dernier
            latest_file = sorted(files)[-1]
            print(f"📂 Fichier le plus récent détecté : {latest_file}")

            # Récupération du contenu
            response = self.storage.client.get_object(self.bucket_bronze, latest_file)
            content = response.read()
            
            # Lecture Pandas
            df = pd.read_csv(io.BytesIO(content))
            print(f"🧹 Nettoyage de {len(df)} lignes...")

            df_cleaned = self.clean_data(df)

            # Export vers Silver (on garde la structure de dossier)
            silver_filename = latest_file.split('/')[-1].replace('.csv', '_cleaned.csv')
            silver_path = f"ecommerce/{silver_filename}"
            
            csv_buffer = df_cleaned.to_csv(index=False).encode('utf-8')
            
            self.storage.client.put_object(
                bucket_name=self.bucket_silver,
                object_name=silver_path,
                data=io.BytesIO(csv_buffer),
                length=len(csv_buffer),
                content_type="text/csv"
            )
            print(f"✅ [SILVER] Données nettoyées envoyées vers : {silver_path}")

        except Exception as e:
            print(f"❌ Erreur lors du traitement : {str(e)}")

if __name__ == "__main__":
    EcommerceCleaner().run()