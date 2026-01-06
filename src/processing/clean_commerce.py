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
        self.source_prefix = "scraping/ecommerce/"

    def clean_data(self, df):
        df = df.drop_duplicates(subset=['sku'])
        df.loc[:, 'price'] = pd.to_numeric(df['price'], errors='coerce')

        if 'description' in df.columns:
            df['description'] = df['description'].str.replace(r'[\r\n]+', ' ', regex=True).str.strip()

        df['imported_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return df

    def run(self):
        try:
            objects = self.storage.client.list_objects(
                self.bucket_bronze, 
                prefix=self.source_prefix, 
                recursive=True
            )
            
            files = [obj.object_name for obj in objects if obj.object_name.endswith('.csv')]
            if not files:
                print(f"Status: Error | Message: No files in {self.source_prefix}")
                return

            latest_file = sorted(files)[-1]
            response = self.storage.client.get_object(self.bucket_bronze, latest_file)
            content = response.read()
            
            df = pd.read_csv(io.BytesIO(content))
            df_cleaned = self.clean_data(df)

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
            print(f"Status: Success | Exported: {self.bucket_silver}/{silver_path}")

        except Exception as e:
            print(f"Status: Failure | Message: {str(e)}")

if __name__ == "__main__":
    EcommerceCleaner().run()