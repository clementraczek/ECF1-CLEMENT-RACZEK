import io
import pandas as pd
from sqlalchemy import create_engine, text
from src.storage.minio_client import MinioClient
from config.settings import minio_config

class PostgresLoader:
    def __init__(self):
        self.user = "dataeng"
        self.password = "dataeng123"
        self.host = "localhost" 
        self.port = "5433"
        self.db = "analytics"
        
        connection_url = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
        self.engine = create_engine(connection_url)
        self.storage = MinioClient()
        print(f"Status: Connected | Host: {self.host}:{self.port}")

    def load_partners_to_gold(self):
        files = self.storage.list_exports(bucket=minio_config.bucket_silver)
        partner_files = [f for f in files if "librairies_cleaned" in f['name']]
        
        if not partner_files:
            print("Status: Error | Message: No silver data found")
            return

        latest_silver = max(partner_files, key=lambda x: x['modified'])['name']
        raw_data = self.storage.get_export(latest_silver, bucket=minio_config.bucket_silver)
        
        if latest_silver.endswith('.xlsx'):
            df = pd.read_excel(io.BytesIO(raw_data))
        else:
            df = pd.read_csv(io.BytesIO(raw_data))

        try:
            df.to_sql("dim_partners", self.engine, if_exists="replace", index=False)
            
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM dim_partners"))
                count = result.scalar()
                print(f"Status: Success | Table: dim_partners | Records: {count}")
                
        except Exception as e:
            print(f"Status: Failure | Message: {str(e)}")

if __name__ == "__main__":
    loader = PostgresLoader()
    loader.load_partners_to_gold()