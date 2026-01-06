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
        if pd.isna(text):
            return ""
        
        text = str(text)
        bad_chars = {
            '“': '', '”': '', '‘': "'", '’': "'", 
            '„': '', '«': '', '»': '', '—': '-', '–': '-'
        }
        for char, replacement in bad_chars.items():
            text = text.replace(char, replacement)
        
        return re.sub(r'\s+', ' ', text).strip()

    def clean_data(self, df):
        required_cols = ['text', 'author', 'tags']
        for col in required_cols:
            if col not in df.columns:
                df[col] = ""

        df['text'] = df['text'].apply(self.clean_text)
        df['author'] = df['author'].apply(lambda x: str(x).strip() if pd.notna(x) else "Unknown")
        df['tags'] = df['tags'].apply(lambda x: str(x).lower().replace(' ,', ',').strip() if pd.notna(x) else "")
        df = df.drop_duplicates(subset=['text', 'author'])
        df['cleaned_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        return df

    def run(self):
        response = None
        try:
            objects = self.storage.client.list_objects(
                self.bucket_bronze, prefix=self.source_prefix, recursive=True
            )
            files = [obj.object_name for obj in objects if obj.object_name.endswith('.csv')]

            if not files:
                print(f"Status: Error | Message: No files in {self.source_prefix}")
                return

            latest_file = sorted(files)[-1]
            response = self.storage.client.get_object(self.bucket_bronze, latest_file)
            df = pd.read_csv(io.BytesIO(response.read()), encoding='utf-8-sig')
            
            df_cleaned = self.clean_data(df)

            silver_filename = latest_file.split('/')[-1].replace('.csv', '_cleaned.csv')
            silver_path = f"quotes/{silver_filename}"
            csv_buffer = df_cleaned.to_csv(index=False, encoding='utf-8').encode('utf-8')
            
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
        finally:
            if response:
                response.close()
                response.release_conn()

if __name__ == "__main__":
    QuotesCleaner().run()