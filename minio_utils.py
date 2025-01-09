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
        
    def list_objects(self, bucket: str, prefix: Optional[str] = None) -> list:
        """
        List objects in a MinIO bucket with optional prefix filtering

        Args:
            bucket: Bucket name
            prefix: Optional prefix to filter objects

        Returns:
            List of objects in the bucket
        """
        try:
            # Check if bucket exists
            if not self.client.bucket_exists(bucket):
                logger.error(f"Bucket {bucket} does not exist")
                raise S3Error(f"Bucket {bucket} does not exist")

            # List objects with prefix if provided
            objects = self.client.list_objects(bucket, prefix=prefix, recursive=True)
            return list(objects)  # Convert iterator to list

        except S3Error as e:
            logger.error(f"Error listing objects from MinIO: {e}")
            raise

    def get_object_metadata(self, bucket: str, object_name: str) -> dict:
        """
        Get metadata for a specific object in MinIO

        Args:
            bucket: Bucket name
            object_name: Object name in MinIO

        Returns:
            Dictionary containing object metadata
        """
        try:
            # Get object stats
            obj_stats = self.client.stat_object(bucket, object_name)
            
            # Extract relevant metadata
            metadata = {
                "size": obj_stats.size,
                "etag": obj_stats.etag,
                "last_modified": obj_stats.last_modified,
                "content_type": obj_stats.content_type,
                "metadata": obj_stats.metadata  # User-defined metadata
            }
            
            return metadata

        except S3Error as e:
            logger.error(f"Error getting object metadata from MinIO: {e}")
            raise

    def remove_object(self, bucket: str, object_name: str) -> None:
        """
        Remove an object from MinIO

        Args:
            bucket: Bucket name
            object_name: Object name in MinIO
        """
        try:
            # Check if object exists
            try:
                self.client.stat_object(bucket, object_name)
            except S3Error as e:
                logger.error(f"Object {object_name} does not exist in bucket {bucket}")
                raise

            # Remove the object
            self.client.remove_object(bucket, object_name)
            logger.info(f"Successfully removed object {object_name} from bucket {bucket}")

        except S3Error as e:
            logger.error(f"Error removing object from MinIO: {e}")
            raise

    def copy_object(self, source_bucket: str, source_object: str, 
                   dest_bucket: str, dest_object: str) -> None:
        """
        Copy an object within MinIO

        Args:
            source_bucket: Source bucket name
            source_object: Source object name
            dest_bucket: Destination bucket name
            dest_object: Destination object name
        """
        try:
            # Ensure source object exists
            try:
                self.client.stat_object(source_bucket, source_object)
            except S3Error:
                raise S3Error(f"Source object {source_object} does not exist in bucket {source_bucket}")

            # Create destination bucket if it doesn't exist
            if not self.client.bucket_exists(dest_bucket):
                self.client.make_bucket(dest_bucket)
                logger.info(f"Created destination bucket: {dest_bucket}")

            # Copy the object
            self.client.copy_object(
                dest_bucket,
                dest_object,
                f"{source_bucket}/{source_object}"
            )
            logger.info(f"Successfully copied {source_bucket}/{source_object} to {dest_bucket}/{dest_object}")

        except S3Error as e:
            logger.error(f"Error copying object in MinIO: {e}")
            raise

    def list_buckets(self) -> list:
        """
        List all buckets in MinIO

        Returns:
            List of bucket names
        """
        try:
            buckets = self.client.list_buckets()
            return [bucket.name for bucket in buckets]
        except S3Error as e:
            logger.error(f"Error listing buckets: {e}")
            raise

    def remove_bucket(self, bucket: str, force: bool = False) -> None:
        """
        Remove a bucket from MinIO

        Args:
            bucket: Bucket name
            force: If True, remove all objects in bucket before removing bucket
        """
        try:
            if not self.client.bucket_exists(bucket):
                logger.error(f"Bucket {bucket} does not exist")
                raise S3Error(f"Bucket {bucket} does not exist")

            if force:
                # Remove all objects first
                objects = self.client.list_objects(bucket, recursive=True)
                for obj in objects:
                    self.client.remove_object(bucket, obj.object_name)
                    logger.info(f"Removed object {obj.object_name} from bucket {bucket}")

            # Remove the bucket
            self.client.remove_bucket(bucket)
            logger.info(f"Successfully removed bucket {bucket}")

        except S3Error as e:
            logger.error(f"Error removing bucket: {e}")
            raise