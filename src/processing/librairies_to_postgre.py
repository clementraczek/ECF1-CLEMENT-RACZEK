import io
import pandas as pd
from sqlalchemy import create_engine, text
from src.storage.minio_client import MinioClient
from config.settings import minio_config
import os

class PostgresLoader:
    def __init__(self):
        # Paramètres d'accès configurés pour l'accès externe (Windows -> Docker)
        self.user = "dataeng"
        self.password = "dataeng123"
        self.host = "localhost" 
        self.port = "5433"  # Port modifié suite au conflit 5432
        self.db = "analytics"
        
        # Construction de l'URL SQLAlchemy
        connection_url = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
        
        # Initialisation de l'engine et du client MinIO
        self.engine = create_engine(connection_url)
        self.storage = MinioClient()
        print(f"🔗 Connexion établie à {self.host}:{self.port}/{self.db} (User: {self.user})")

    def load_partners_to_gold(self):
        """Récupère le fichier Silver et l'injecte en zone Gold (PostgreSQL)"""
        
        # 1. Recherche du dernier fichier nettoyé dans le bucket Silver
        files = self.storage.list_exports(bucket=minio_config.bucket_silver)
        partner_files = [f for f in files if "librairies_cleaned" in f['name']]
        
        if not partner_files:
            print("⚠️ Erreur : Aucun fichier 'librairies_cleaned' trouvé dans Silver.")
            return

        # On prend le fichier le plus récent basé sur la date de modification
        latest_silver = max(partner_files, key=lambda x: x['modified'])['name']
        print(f"📦 Chargement du fichier : {latest_silver}")

        # 2. Chargement des données en DataFrame
        raw_data = self.storage.get_export(latest_silver, bucket=minio_config.bucket_silver)
        
        # Gestion du format (CSV par défaut pour Silver)
        if latest_silver.endswith('.xlsx'):
            df = pd.read_excel(io.BytesIO(raw_data))
        else:
            df = pd.read_csv(io.BytesIO(raw_data))

        # 3. Injection dans PostgreSQL
        # if_exists='replace' permet de recréer la table avec les nouvelles colonnes (ex: tranche_ca)
        try:
            df.to_sql("dim_partners", self.engine, if_exists="replace", index=False)
            
            # Vérification du nombre de lignes
            with self.engine.connect() as conn:
                # SQLAlchemy 2.0 nécessite de committer ou d'utiliser un bloc de transaction
                result = conn.execute(text("SELECT COUNT(*) FROM dim_partners"))
                count = result.scalar()
                print(f"✅ [GOLD] Succès ! Table 'dim_partners' mise à jour avec {count} lignes.")
                
        except Exception as e:
            print(f"❌ Erreur lors de l'injection SQL : {e}")

if __name__ == "__main__":
    loader = PostgresLoader()
    loader.load_partners_to_gold()