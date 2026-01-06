import pandas as pd
import io
from datetime import datetime
from src.storage.minio_client import MinioClient

excel_file_path = "data/partenaire_librairies.xlsx"
client = MinioClient()

def run_import():
    try:
        df = pd.read_excel(excel_file_path)
        
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Partenaires")
        
        buffer_size = len(excel_buffer.getvalue())
        excel_buffer.seek(0)

        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        filename = f"partenaire_librairies_{timestamp}.xlsx"
        object_path = f"adresses/{filename}"

        client.client.put_object(
            bucket_name="bronze",
            object_name=object_path,
            data=excel_buffer,
            length=buffer_size,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        print(f"Status: Export success | Target: bronze/{object_path}")
        
    except Exception as e:
        print(f"Status: Execution error | Message: {str(e)}")

if __name__ == "__main__":
    run_import()