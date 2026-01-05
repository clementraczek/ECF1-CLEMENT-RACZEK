import pandas as pd
import io
import requests
import hashlib
import time
from datetime import datetime
from src.storage.minio_client import MinioClient
from config.settings import minio_config

class PartnerCleaner:
    def __init__(self):
        self.storage = MinioClient()
        self.api_url = "https://api-adresse.data.gouv.fr/search/"

    def geocode_address(self, row):
        """EF2 : Appel API Adresse"""
        query = f"{row['adresse']} {row['code_postal']} {row['ville']}"
        try:
            res = requests.get(self.api_url, params={'q': query, 'limit': 1}, timeout=5)
            if res.status_code == 200:
                data = res.json()
                if data['features']:
                    coords = data['features'][0]['geometry']['coordinates']
                    return pd.Series([coords[1], coords[0]]) # Lat, Lon
            time.sleep(0.1)
        except Exception:
            pass
        return pd.Series([None, None])

    def apply_rgpd(self, df):
        """Livrable 2.3 : Pseudonymisation"""
        # Hachage de l'email pour l'anonymat technique
        if 'contact_email' in df.columns:
            df['contact_id'] = df['contact_email'].apply(
                lambda x: hashlib.sha256(str(x).encode()).hexdigest()[:12] if pd.notnull(x) else None
            )
        # Suppression des colonnes Identifiantes (PII)
        to_drop = ['contact_nom', 'contact_email', 'contact_telephone']
        return df.drop(columns=[c for c in to_drop if c in df.columns])

    def process_to_silver(self):
        # 1. Lister les fichiers dans Bronze pour trouver le plus récent
        files = self.storage.list_exports(bucket=minio_config.bucket_bronze)
        partner_files = [f for f in files if "partenaire_librairies" in f['name']]
        
        if not partner_files:
            print("⚠️ Aucun fichier trouvé dans Bronze.")
            return

        latest_file = max(partner_files, key=lambda x: x['modified'])['name']
        print(f"Processing : {latest_file}")

        # 2. Lecture du fichier Excel
        raw_data = self.storage.get_export(latest_file, bucket=minio_config.bucket_bronze)
        df = pd.read_excel(io.BytesIO(raw_data))

        # 3. Enrichissement & Nettoyage
        print("🌍 Géocodage en cours...")
        df[['lat', 'lon']] = df.apply(self.geocode_address, axis=1)
        
        print("🛡️ Application RGPD...")
        df_clean = self.apply_rgpd(df)

        # 4. Export vers SILVER en CSV
        csv_buffer = df_clean.to_csv(index=False).encode('utf-8')
        silver_path = f"partners/librairies_cleaned_{datetime.now().strftime('%Y%m%d')}.csv"
        
        self.storage.client.put_object(
            bucket_name=minio_config.bucket_silver,
            object_name=silver_path,
            data=io.BytesIO(csv_buffer),
            length=len(csv_buffer),
            content_type="text/csv"
        )
        print(f"✅ [SILVER] Données nettoyées uploadées : {silver_path}")

if __name__ == "__main__":
    cleaner = PartnerCleaner()
    cleaner.process_to_silver()