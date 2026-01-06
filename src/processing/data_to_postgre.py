import io
import pandas as pd
from sqlalchemy import create_engine, text
from src.storage.minio_client import MinioClient
from config.settings import minio_config

class GoldLoader:
    def __init__(self):
        self.user = "dataeng"
        self.password = "dataeng123"
        self.host = "localhost"
        self.port = "5433"
        self.db = "analytics"
        
        url = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
        self.engine = create_engine(url)
        self.storage = MinioClient()

    def clean_all_views(self):
        sql = """
        DO $$ DECLARE
            r RECORD;
        BEGIN
            FOR r IN (SELECT viewname FROM pg_views WHERE schemaname = 'public') LOOP
                EXECUTE 'DROP VIEW IF EXISTS ' || quote_ident(r.viewname) || ' CASCADE';
            END LOOP;
        END $$;
        """
        with self.engine.begin() as conn:
            conn.execute(text(sql))
            print("Status: Views cleaned (CASCADE)")

    def get_latest_silver(self, prefix):
        objects = self.storage.client.list_objects(minio_config.bucket_silver, prefix=prefix, recursive=True)
        files = [obj.object_name for obj in objects if obj.object_name.endswith('_cleaned.csv')]
        return sorted(files)[-1] if files else None

    def load_table(self, silver_prefix, table_name):
        file_path = self.get_latest_silver(silver_prefix)
        if not file_path:
            print(f"Status: Skip | Message: No data for {silver_prefix}")
            return

        response = self.storage.client.get_object(minio_config.bucket_silver, file_path)
        df = pd.read_csv(io.BytesIO(response.read()))
        
        df.to_sql(table_name, self.engine, if_exists="replace", index=False)
        
        with self.engine.connect() as conn:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            print(f"Status: Success | Table: {table_name} | Records: {count}")
        
        response.close()
        response.release_conn()

    def run(self):
        self.clean_all_views()
        self.load_table("books/", "fact_books")
        self.load_table("quotes/", "fact_quotes")
        self.load_table("ecommerce/", "fact_products")

if __name__ == "__main__":
    GoldLoader().run()