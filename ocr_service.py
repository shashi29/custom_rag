import os
import uuid
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import traceback
from urllib.parse import urlparse

import redis
from dotenv import load_dotenv
from rabbitmq_utils import RabbitMQClient
# from s3_utils import S3Client
from extractors import DocumentExtractor
from sentence_transformers import SentenceTransformer
import zlib
import numpy as np

from qdrant_client.http.models import PointStruct, Filter, FieldCondition, MatchValue, UpdateStatus
from qdrant_client.http import models
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct, VectorParams, Distance, UpdateStatus, OptimizersConfigDiff
from rabbitmq_utils import RabbitMQClient
from embedding_client import EmbeddingServiceClient
from minio_utils import MinioClient
from chonkie import LateChunker

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ProcessingStatus(Enum):
    """Enhanced enum for processing status"""
    QUEUED = "QUEUED"
    STARTED = "STARTED"
    PROCESSING = "PROCESSING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    MESSAGE_QUEUED = "MESSAGE_QUEUED"
    MESSAGE_SENT = "MESSAGE_SENT"
    MESSAGE_FAILED = "MESSAGE_FAILED"
    
    def __str__(self):
        return self.value

class ServiceConfig:
    """Service configuration from environment variables"""
    
    def __init__(self):
        # RabbitMQ Configuration
        self.rabbitmq_host = self._get_env("RABBITMQ_HOST")
        self.input_queue = self._get_env("RABBITMQ_QUEUE")
        # self.output_queue = self._get_env("OUTPUT_QUEUE")
        self.rabbitmq_user = self._get_env("RABBITMQ_USER")
        self.rabbitmq_pass = self._get_env("RABBITMQ_PASSWORD")
        
        # S3 Configuration
        # self.s3_access_key = self._get_env("AWS_ACCESS_KEY_ID")
        # self.s3_secret_key = self._get_env("AWS_SECRET_ACCESS_KEY")
        
        # MinIO Configuration
        self.minio_endpoint = self._get_env("MINIO_ENDPOINT")
        self.minio_access_key = self._get_env("MINIO_ACCESS_KEY")
        self.minio_secret_key = self._get_env("MINIO_SECRET_KEY")
        self.minio_secure = self._get_env("MINIO_SECURE", "True").lower() == "true"
        
        # Redis Configuration
        self.redis_host = self._get_env("REDIS_HOST")
        self.redis_port = int(self._get_env("REDIS_PORT"))
        self.redis_db = int(self._get_env("REDIS_DB"))
        
        # Service Configuration
        self.service_name = self._get_env("SERVICE_NAME", "OCRPreProcessing")
        self.max_retries = int(self._get_env("MAX_RETRIES", "3"))
        self.retry_delay = int(self._get_env("RETRY_DELAY", "5"))
        self.status_check_timeout = int(self._get_env("STATUS_CHECK_TIMEOUT", "7200"))
        self.temp_dir = self._get_env("TEMP_DIR", "/tmp")
        
        #Qdrant settings
        self.qdrant_url = self._get_env("QDRANT_URL")
        self.embedding_model = self._get_env("EMBEDDING_MODEL")
        
    @staticmethod
    def _get_env(key: str, default: Optional[str] = None) -> str:
        value = os.getenv(key, default)
        if value is None:
            raise ValueError(f"Environment variable {key} is not set")
        return value

@dataclass
class ProcessingEvent:
    """Data class for processing events"""
    service_name: str
    cid: str
    organization: str
    status: ProcessingStatus
    datetime: str
    details: Dict[str, Any]
    error: Optional[str] = None
    error_description: Optional[str] = None
    retry_count: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert ProcessingEvent to a Redis-compatible dictionary"""
        data = asdict(self)
        data['status'] = str(data['status'])
        data['details'] = str(data['details'])  # Convert dict to string for Redis
        return {k: str(v) if v is not None else '' for k, v in data.items()}

class RedisClient:
    """Enhanced Redis client for storing processing events"""
    
    def __init__(self, host: str, port: int, db: int):
        self.client = redis.Redis(host=host, port=port, db=db)
    
    def get_latest_status(self, cid: str) -> Tuple[Optional[str], Optional[str]]:
        """Get latest processing status for a CID"""
        key = f"status:{cid}"
        status_data = self.client.hgetall(key)
        
        if not status_data:
            return None, None
            
        status = status_data.get(b'status', b'').decode()
        timestamp = status_data.get(b'timestamp', b'').decode()
        return status, timestamp
    
    def store_event(self, event: ProcessingEvent) -> None:
        """Store processing event in Redis"""
        key = f"status:{event.cid}"
        event_dict = event.to_dict()
        self.client.hset(key, event_dict)
        # self.client.expire(key, 2592000)  # 30 days expiry

class ProcessingService:
    """Enhanced OCR Processing Service"""
    
    def __init__(self):
        self.config = ServiceConfig()
        self.input_queue = RabbitMQClient(self.config.rabbitmq_host, 
                                          self.config.input_queue, 
                                          self.config.rabbitmq_user, 
                                          self.config.rabbitmq_pass)
        
        self.minio_client = MinioClient(
            endpoint=self.config.minio_endpoint,
            access_key=self.config.minio_access_key,
            secret_key=self.config.minio_secret_key,
            secure=self.config.minio_secure
        )
        self.redis_client = RedisClient(
            self.config.redis_host,
            self.config.redis_port,
            self.config.redis_db
        )

        os.makedirs(self.config.temp_dir, exist_ok=True)
        
        self.qdrant_client = QdrantClient(url=self.config.qdrant_url)
        self.embedding_service = EmbeddingServiceClient()

    def log_event(
        self,
        service_name: str,
        cid: str,
        organization: str,
        status: ProcessingStatus,
        details: Dict[str, Any],
        error: Optional[str] = None,
        error_description: Optional[str] = None,
        retry_count: Optional[int] = None
    ) -> None:
        """Log processing event to Redis"""
        event = ProcessingEvent(
            service_name=service_name,
            cid=cid,
            organization=organization,
            status=status,
            datetime=datetime.utcnow().isoformat(),
            details=details,
            error=error,
            error_description=error_description,
            retry_count=retry_count
        )
        self.redis_client.store_event(event)
        logger.info(f"Logged event: {event}")

    def process_message(self, message: Dict[str, Any]) -> None:
        """Process a single message with enhanced error handling and status tracking"""
        message_id = str(uuid.uuid4())
        local_input_path = None
        local_output_path = None
        metadata = message.get('metadata', {})
        organization = message.get('collection_name', '')

        try:
            logger.info(f"Processing message: {message}")

            # Process the document
            result_flag = self._process_single_document(message, local_input_path, local_output_path)
            
            logger.info(f"Successfully processed document for CID: {message_id}")

        except Exception as e:
            error_description = traceback.format_exc()
            logger.error(f"Error processing message: {str(e)}\n{error_description}")
            
            self.log_event(
                service_name="ResumeService",
                cid=message_id,
                organization=organization,
                status=ProcessingStatus.FAILED,
                details=message,
                error="ProcessingError",
                error_description=str(error_description)
            )

        finally:
            self._clean_up_temp_files(local_input_path, local_output_path)

    def _process_single_document(
        self,
        message: Dict[str, Any],
        local_input_path: Optional[str],
        local_output_path: Optional[str]
    ) -> Dict[str, Any]:
        """Process a single document with enhanced path handling"""
        input_path = message['storage_path']
        metadata = message.get('metadata', {})
        
        # Parse MinIO path
        input_bucket, input_key = self.minio_client.parse_uri(input_path)
        
        # Generate filenames and paths
        original_filename = os.path.basename(input_key)
        filename_without_ext, _ = os.path.splitext(original_filename)
        output_filename = f"{filename_without_ext}.txt"
        
        local_input_path = os.path.join(self.config.temp_dir, original_filename)
        local_output_path = os.path.join(self.config.temp_dir, output_filename)
        
        # Generate output MinIO key
        output_key = self._generate_output_key(input_key, output_filename)
        organization = message.get('collection_name', '')

        # Download and process
        try:
            self.minio_client.download_file(input_bucket, input_key, local_input_path)

            # Extract text
            if original_filename.lower().endswith('.pdf'):
                text_list = DocumentExtractor.extract_from_pdf(local_input_path)
            else:
                text_list = DocumentExtractor.extract_from_doc(local_input_path)
        except Exception as e:
            logger.error(f"Error extracting text from document: {e}")
            text = "No text extracted"

        # Insert in Vector Database
        self.create_collection_if_not_exists(organization, 384)

        chunker = LateChunker(
            embedding_model="all-MiniLM-L6-v2",
            mode = "sentence",
            chunk_size=512,
            min_sentences_per_chunk=1,
            min_characters_per_sentence=12,
        )
                
        for page_info in text_list:
            print(page_info.text)
            print("Page Number: ", page_info.page_number)
            
            text = page_info.text            
            # Run the embedding service on the file
            #embedding = self.embedding_service.process_with_polling(text, priority="low") 
            chunks = chunker(text)

            for chunk in chunks:
                print(f"Chunk text: {chunk.text}")
                embedding = chunk.embedding

                # Insert embedding into Qdrant
                payload = {
                    "storage_path": message.get('storage_path'),
                    "text": chunk.text,
                    "page_number": page_info.page_number,
                    **metadata
                }
            
                # self.delete_embedding(organization, embedding, payload)
                
                self.insert_embedding(organization, embedding, payload)
                
        return True        

    def create_collection_if_not_exists(self, collection_name: str, vector_size: int):
        """Create a Qdrant collection if it doesn't exist."""
        if not self.check_collection_exists(collection_name):
            try:
                self.qdrant_client.create_collection(
                    collection_name=collection_name,
                    vectors_config=VectorParams(
                        size=vector_size, 
                        distance=Distance.COSINE,
                        on_disk=True
                    ),
                    optimizers_config=OptimizersConfigDiff(indexing_threshold=20000)
                )
                logger.info(f"Collection '{collection_name}' created successfully.")
            except Exception as e:
                logger.error(f"Error creating collection '{collection_name}': {e}")
                raise

    def check_collection_exists(self, collection_name: str) -> bool:
        """Check if the collection already exists in Qdrant."""
        try:
            collections = self.qdrant_client.get_collections()
            return any(collection.name == collection_name for collection in collections.collections)
        except Exception as e:
            logger.error(f"Error checking collection existence for '{collection_name}': {e}")
            raise

    def delete_embedding(self, collection_name:str, vector: list, payload: dict):
        try:
            status = payload.get("status", None)
            if status != "AVAILABLE":
                cid = payload.get("_id", None)
                if cid:
                    #Then delete the points 
                    self.qdrant_client.delete(
                    collection_name=collection_name,
                    points_selector=models.FilterSelector(
                        filter=models.Filter(
                            must=[
                                models.FieldCondition(
                                    key="_id",
                                    match=models.MatchValue(value=cid),
                                ),
                            ],
                        )
                    ),
                    )

        except Exception as e:
            logger.error(f"Error inserting embedding into collection '{collection_name}': {e}")
            raise

    def insert_embedding(self, collection_name: str, vector: list, payload: dict):
        """Insert a single embedding into the specified Qdrant collection."""
        try:
            point = PointStruct(id=str(uuid.uuid4()), vector=vector, payload=payload)
            operation_info = self.qdrant_client.upsert(
                collection_name=collection_name,
                wait=True,
                points=[point]
            )
            if operation_info.status != UpdateStatus.COMPLETED:
                raise Exception(f"Failed to insert embedding into collection '{collection_name}'.")
            else:
                logger.info("Skipping the Resume as status is not equal to available")
        except Exception as e:
            logger.error(f"Error inserting embedding into collection '{collection_name}': {e}")
            raise
        
    def _generate_output_key(self, input_key: str, output_filename: str) -> str:
        """Generate output key with OCR folder structure"""
        path_components = input_key.split('/')
        last_folder_index = len(path_components) - 2
        path_components[last_folder_index] = "ocr"
        path_components[-1] = output_filename
        return '/'.join(path_components[:-1]) + '/' + output_filename

    def _clean_up_temp_files(self, *file_paths: str) -> None:
        """Clean up temporary files"""
        for file_path in file_paths:
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    logger.debug(f"Cleaned up file: {file_path}")
                except Exception as e:
                    logger.warning(f"Failed to clean up file {file_path}: {e}")

    def start(self) -> None:
        """Start the OCR Processing Service"""
        logger.info("Starting OCR Processing Service")
        self.input_queue.start_consumer(self.process_message)

    def check_health(self) -> bool:
        """Check service health"""
        try:
            # Check Redis connection
            #self.redis_client.client.ping()
            
            # Check RabbitMQ connections
            rabbitmq_health = self.input_queue.check_health(),
            
            return rabbitmq_health
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

def main():
    """Main entry point"""
    try:
        service = ProcessingService()
        
        if service.check_health():
            logger.info("Health check passed. Starting OCR Processing Service.")
            service.start()
        else:
            logger.error("Health check failed. Please check your connections.")
            exit(1)
    except Exception as e:
        logger.critical(f"Service failed to start: {e}")
        exit(1)

if __name__ == "__main__":
    main()