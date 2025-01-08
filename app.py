from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from minio_utils import MinioClient
import os, uuid, json
from datetime import datetime
import tempfile
from typing import Optional, Dict, Any, Tuple
from pydantic import BaseModel
from rabbitmq_utils import RabbitMQClient
import redis
from ocr_service import ProcessingEvent

class FileUploadRequest(BaseModel):
    organization_id: str  
    metadata: Optional[Dict[str, Any]] = {}

app = FastAPI()

minio_client = MinioClient(
    endpoint="localhost:9000",
    access_key="BVxA5YuSF5vkzXkXMym7", 
    secret_key="jLDOAIEsfef50DK90gIPb5ucON7K1WuC93dOKa8F",
    secure=False
)

class RedisClient:
    """Enhanced Redis client for storing processing events"""
    
    def __init__(self, host: str, port: int, db: int):
        self.client = redis.Redis(host=host, port=port, db=db)
    
    def get_latest_status(self, service_name: str, cid: str) -> Tuple[Optional[str], Optional[str]]:
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
        self.client.hmset(key, event_dict)
        # self.client.expire(key, 2592000)  # 30 days expiry
        
redis_client = RedisClient(
    "localhost",
    "6379",
    "0"
)

@app.post("/submit-job")
async def submit_job(
    file: UploadFile = File(...),
    organization_id: str = Form(...),
    metadata: str = Form("{}")
):
    temp_file = None
    # try:
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    file_content = await file.read()
    temp_file.write(file_content)
    temp_file.close()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    object_name = f"{organization_id}/{timestamp}_{file.filename}"
    bucket_name = "documents"

    minio_client.upload_file(bucket_name, object_name, temp_file.name)
    storage_path = f"minio://{bucket_name}/{object_name}"

    job_id = str(uuid.uuid4())
    message = {
        "job_id": job_id,
        "collection_name": organization_id,
        "storage_path": storage_path,
        "metadata": json.loads(metadata)
    }
    rabbitmq_client = RabbitMQClient(
            host="localhost",
            queue="custom_rag",
            user="cwRI82uX5HyT",
            password="9j4u6PluofN5"
    )

    rabbitmq_client.send_message(message, 0)
    # redis_client.hmset(
    #     f"status:{job_id}",
    #     mapping={
    #         "status": "QUEUED",
    #         "organization_id": organization_id,
    #         "storage_path": storage_path,
    #         "timestamp": str(datetime.utcnow())
    #     }
    # )

    return {
        "job_id": job_id,
        "status": "queued",
        "storage_path": storage_path
    }

