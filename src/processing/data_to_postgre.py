import io
import pandas as pd
from sqlalchemy import create_engine, text
from src.storage.minio_client import MinioClient
from config.settings import minio_config

class GoldLoader:
    def __init__(self):
        # Configuration PostgreSQL
        self.user = "dataeng"
        self.password = "dataeng123"
        self.host = "localhost"
        self.port = "5433"
        self.db = "analytics"
        
        url = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
        self.engine = create_engine(url)
        self.storage = MinioClient()

    def get_latest_silver(self, prefix):
        """Récupère le dernier fichier CSV nettoyé dans Silver pour un dossier donné"""
        objects = self.storage.client.list_objects(minio_config.bucket_silver, prefix=prefix, recursive=True)
        files = [obj.object_name for obj in objects if obj.object_name.endswith('_cleaned.csv')]
        return sorted(files)[-1] if files else None

    def load_table(self, silver_prefix, table_name):
        """Lit un CSV Silver et l'injecte dans Postgres"""
        file_path = self.get_latest_silver(silver_prefix)
        if not file_path:
            print(f"⚠️ Aucun fichier trouvé pour {silver_prefix}")
            return

        print(f"📦 Injection de {file_path} vers table '{table_name}'...")
        
        response = self.storage.client.get_object(minio_config.bucket_silver, file_path)
        df = pd.read_csv(io.BytesIO(response.read()))
        
        # Injection SQL
        df.to_sql(table_name, self.engine, if_exists="replace", index=False)
        
        with self.engine.connect() as conn:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            print(f"✅ Table '{table_name}' mise à jour : {count} lignes.")
        
        response.close()
        response.release_conn()

    def run(self):
        # On charge les 3 sources
        self.load_table("books/", "fact_books")
        self.load_table("quotes/", "fact_quotes")
        self.load_table("ecommerce/", "fact_products")

if __name__ == "__main__":
    GoldLoader().run()