"""
Client MinIO pour le stockage d'objets.

Ce module fournit une interface simplifiée pour interagir avec MinIO :
- Upload/download de fichiers (CSV, JSON, Parquet)
- Gestion des backups
- Génération d'URLs présignées

MinIO est un serveur de stockage objet compatible S3.
"""

import io
import json
from datetime import datetime, timedelta
from typing import Optional
from minio import Minio
from minio.error import S3Error
import structlog

# Import de la configuration
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from config.settings import minio_config

logger = structlog.get_logger()


class MinioClient:
    """
    Gestionnaire de stockage MinIO pour les buckets bronze, silver et gold.
    """

    def __init__(self):
        """Initialise le client MinIO et crée les buckets nécessaires."""
        self.client = Minio(
            endpoint=minio_config.endpoint,
            access_key=minio_config.access_key,
            secret_key=minio_config.secret_key,
            secure=minio_config.secure
        )
        self._ensure_buckets()

    def _ensure_buckets(self) -> None:
        """Crée les buckets bronze, silver et gold s'ils n'existent pas."""
        buckets = [
            minio_config.bucket_bronze,
            minio_config.bucket_silver,
            minio_config.bucket_gold
        ]
        for bucket in buckets:
            try:
                if not self.client.bucket_exists(bucket):
                    self.client.make_bucket(bucket)
                    logger.info("bucket_created", bucket=bucket)
            except S3Error as e:
                logger.error("bucket_creation_failed", bucket=bucket, error=str(e))

    # ==================== EXPORTS ====================

    def upload_export(
        self,
        data: bytes,
        filename: str,
        content_type: str = "application/octet-stream"
    ) -> Optional[str]:
        """
        Upload un fichier d'export (CSV, JSON, Parquet) dans le bucket bronze.

        Si le fichier existe déjà, il sera **écrasé**, donc pas de doublons.

        Args:
            data: Contenu du fichier
            filename: Nom du fichier (ex: "books_20260105.csv")
            content_type: Type MIME

        Returns:
            URI MinIO ou None en cas d'erreur
        """
        try:
            # On met toujours le fichier au même emplacement dans le bucket bronze
            bucket = minio_config.bucket_bronze

            self.client.put_object(
                bucket_name=bucket,
                object_name=filename,
                data=io.BytesIO(data),
                length=len(data),
                content_type=content_type
            )

            uri = f"minio://{bucket}/{filename}"
            logger.info("export_uploaded", filename=filename, size_kb=len(data) // 1024)
            return uri

        except S3Error as e:
            logger.error("export_upload_failed", filename=filename, error=str(e))
            return None


    def upload_csv(self, csv_content: str, filename: str) -> Optional[str]:
        """Upload un fichier CSV."""
        return self.upload_export(csv_content.encode("utf-8"), filename, "text/csv")

    def upload_json(self, data: dict, filename: str) -> Optional[str]:
        """Upload un fichier JSON."""
        json_bytes = json.dumps(data, indent=2, ensure_ascii=False, default=str).encode("utf-8")
        return self.upload_export(json_bytes, filename, "application/json")

    def get_export(self, filename: str, bucket: str = None) -> Optional[bytes]:
        """Télécharge un fichier d'export."""
        target_bucket = bucket or minio_config.bucket_bronze
        try:
            response = self.client.get_object(target_bucket, filename)
            data = response.read()
            response.close()
            response.release_conn()
            return data
        except S3Error:
            return None

    def list_exports(self, bucket: str = None) -> list[dict]:
        """Liste tous les exports dans un bucket donné."""
        target_bucket = bucket or minio_config.bucket_bronze
        try:
            objects = self.client.list_objects(target_bucket, recursive=True)
            return [
                {"name": obj.object_name, "size": obj.size, "modified": obj.last_modified}
                for obj in objects
            ]
        except S3Error as e:
            logger.error("list_exports_failed", bucket=target_bucket, error=str(e))
            return []

    # ==================== STATISTIQUES ====================

    def get_stats(self) -> dict:
        """Retourne des statistiques sur les buckets bronze, silver et gold."""
        stats = {}
        for bucket_name in [minio_config.bucket_bronze,
                            minio_config.bucket_silver,
                            minio_config.bucket_gold]:
            try:
                objects = list(self.client.list_objects(bucket_name, recursive=True))
                stats[bucket_name] = {
                    "count": len(objects),
                    "total_size_mb": sum(o.size for o in objects) / (1024 * 1024)
                }
            except S3Error:
                stats[bucket_name] = {"count": 0, "total_size_mb": 0}
        return stats


# ==================== TEST DU MODULE ====================

if __name__ == "__main__":
    print("Test du client MinIO...")

    storage = MinioClient()

    # Test upload
    test_data = b"Hello MinIO!"
    uri = storage.upload_export(test_data, "test.txt", "text/plain")
    print(f"Upload test: {uri}")

    # Test stats
    stats = storage.get_stats()
    print(f"Stats: {stats}")

    print("Tests terminés!")
