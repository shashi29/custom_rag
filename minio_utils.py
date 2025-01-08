from minio import Minio
from minio.error import S3Error
import os
import logging
from typing import Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

class MinioClient:
    """MinIO client wrapper for object storage operations"""
    
    def __init__(self, endpoint: str, access_key: str, secret_key: str, secure: bool = True):
        """
        Initialize MinIO client
        
        Args:
            endpoint: MinIO server endpoint
            access_key: Access key for authentication
            secret_key: Secret key for authentication
            secure: Use HTTPS if True, HTTP if False
        """
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        
    def download_file(self, bucket: str, object_name: str, file_path: str) -> None:
        """
        Download a file from MinIO
        
        Args:
            bucket: Bucket name
            object_name: Object name in MinIO
            file_path: Local file path to save the downloaded object
        """
        try:
            # Ensure the directory exists
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # Download the object
            self.client.fget_object(bucket, object_name, file_path)
            logger.info(f"Successfully downloaded {object_name} to {file_path}")
            
        except S3Error as e:
            logger.error(f"Error downloading file from MinIO: {e}")
            raise
            
    def upload_file(self, bucket: str, object_name: str, file_path: str) -> None:
        """
        Upload a file to MinIO
        
        Args:
            bucket: Bucket name
            object_name: Object name in MinIO
            file_path: Local file path to upload
        """
        try:
            # Create bucket if it doesn't exist
            if not self.client.bucket_exists(bucket):
                self.client.make_bucket(bucket)
                logger.info(f"Created bucket: {bucket}")
            
            # Upload the file
            self.client.fput_object(bucket, object_name, file_path)
            logger.info(f"Successfully uploaded {file_path} to {bucket}/{object_name}")
            
        except S3Error as e:
            logger.error(f"Error uploading file to MinIO: {e}")
            raise
            
    def parse_uri(self, uri: str) -> tuple[str, str]:
        """
        Parse MinIO URI into bucket and object name
        
        Args:
            uri: MinIO URI (e.g., 'minio://bucket/path/to/object')
            
        Returns:
            Tuple of (bucket_name, object_name)
        """
        parsed = urlparse(uri)
        bucket = parsed.netloc
        object_name = parsed.path.lstrip('/')
        return bucket, object_name
        
    def get_presigned_url(self, bucket: str, object_name: str, expiry: int = 3600) -> str:
        """
        Generate presigned URL for object access
        
        Args:
            bucket: Bucket name
            object_name: Object name in MinIO
            expiry: URL expiry time in seconds (default: 1 hour)
            
        Returns:
            Presigned URL string
        """
        try:
            url = self.client.presigned_get_object(bucket, object_name, expiry)
            return url
        except S3Error as e:
            logger.error(f"Error generating presigned URL: {e}")
            raise