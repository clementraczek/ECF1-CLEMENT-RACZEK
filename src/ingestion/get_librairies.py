import pandas as pd
import io
from datetime import datetime
from storage.minio_client import MinioClient

# Chemin vers le fichier Excel
excel_file_path = "data/partenaire_librairies.xlsx"

# Initialisation du client MinIO
client = MinioClient()

# Lire le fichier Excel
df = pd.read_excel(excel_file_path)

# Créer un buffer Excel en mémoire
excel_buffer = io.BytesIO()
with pd.ExcelWriter(excel_buffer, engine="xlsxwriter") as writer:
    df.to_excel(writer, index=False, sheet_name="Partenaires")
excel_buffer.seek(0)  # Repositionner le buffer au début

# Nom du fichier dans MinIO avec date/heure
filename = f"partenaire_librairies_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
object_path = f"adresses/{filename}"

# Upload vers le bucket silver
try:
    client.client.put_object(
        bucket_name="bronze",
        object_name=object_path,
        data=excel_buffer,
        length=len(excel_buffer.getvalue()),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    print(f"Fichier Excel uploadé avec succès dans le bucket silver : {object_path}")
except Exception as e:
    print(f"Erreur lors de l'upload : {e}")
