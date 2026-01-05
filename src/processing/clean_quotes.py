import pandas as pd
import io
import re
from datetime import datetime
from src.storage.minio_client import MinioClient
from config.settings import minio_config

class QuotesCleaner:
    def __init__(self):
        self.storage = MinioClient()
        self.bucket_bronze = minio_config.bucket_bronze
        self.bucket_silver = minio_config.bucket_silver
        self.source_prefix = "scraping/quotes/"

    def clean_text(self, text):
        """Nettoyage approfondi des caractères spéciaux"""
        if pd.isna(text):
            return ""
        
        text = str(text)
        # 1. Remplacement des guillemets typographiques variés (ouverts, fermés, simples, doubles)
        bad_chars = {
            '“': '', '”': '', '‘': "'", '’': "'", 
            '„': '', '«': '', '»': '', '—': '-', '–': '-'
        }
        for char, replacement in bad_chars.items():
            text = text.replace(char, replacement)
        
        # 2. Suppression des espaces blancs multiples et sauts de ligne
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text

    def clean_data(self, df):
        """Nettoyage des citations scrapées"""
        # S'assurer que les colonnes nécessaires existent
        required_cols = ['text', 'author', 'tags']
        for col in required_cols:
            if col not in df.columns:
                df[col] = ""

        # 1. Application du nettoyage de texte sur la citation et l'auteur
        df['text'] = df['text'].apply(self.clean_text)
        df['author'] = df['author'].apply(lambda x: str(x).strip() if pd.notna(x) else "Unknown")

        # 2. Nettoyage des tags (mise en minuscule et remplacement des virgules par des espaces propres)
        df['tags'] = df['tags'].apply(lambda x: str(x).lower().replace(' ,', ',').strip() if pd.notna(x) else "")

        # 3. Suppression des doublons après nettoyage
        df = df.drop_duplicates(subset=['text', 'author'])

        # 4. Horodatage
        df['cleaned_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        return df

    def run(self):
        print(f"🔍 Scan du dossier '{self.source_prefix}' dans {self.bucket_bronze}...")
        response = None
        try:
            objects = self.storage.client.list_objects(
                self.bucket_bronze, prefix=self.source_prefix, recursive=True
            )
            files = [obj.object_name for obj in objects if obj.object_name.endswith('.csv')]

            if not files:
                print(f"❌ Aucun fichier trouvé dans {self.source_prefix}")
                return

            latest_file = sorted(files)[-1]
            print(f"📂 Fichier détecté : {latest_file}")

            response = self.storage.client.get_object(self.bucket_bronze, latest_file)
            # Ajout de l'encodage utf-8-sig pour gérer d'éventuels BOM Excel
            df = pd.read_csv(io.BytesIO(response.read()), encoding='utf-8-sig')
            
            print(f"🧹 Nettoyage de {len(df)} citations...")
            df_cleaned = self.clean_data(df)

            silver_filename = latest_file.split('/')[-1].replace('.csv', '_cleaned.csv')
            silver_path = f"quotes/{silver_filename}"
            
            # Sauvegarde en UTF-8 sans caractères bizarres
            csv_buffer = df_cleaned.to_csv(index=False, encoding='utf-8').encode('utf-8')
            
            self.storage.client.put_object(
                bucket_name=self.bucket_silver,
                object_name=silver_path,
                data=io.BytesIO(csv_buffer),
                length=len(csv_buffer),
                content_type="text/csv"
            )
            print(f"✅ [SILVER] Données nettoyées avec succès : {silver_path}")

        except Exception as e:
            print(f"❌ Erreur critique : {str(e)}")
        finally:
            if response:
                response.close()
                response.release_conn()

if __name__ == "__main__":
    QuotesCleaner().run()