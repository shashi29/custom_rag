from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from minio_utils import MinioClient
import os, uuid, json
from datetime import datetime
import tempfile
from typing import Optional, Dict, Any, Tuple
from pydantic import BaseModel
from rabbitmq_utils import RabbitMQClient
import redis
from ocr_service import ProcessingEvent, RedisClient
from enum import Enum
from dataclasses import dataclass, asdict
from ocr_service import *
from fastapi import FastAPI, HTTPException, Query
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from minio_utils import MinioClient
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams, OptimizersConfigDiff
import logging
from datetime import datetime
from typing import List

from dotenv import load_dotenv
# Load environment variables
load_dotenv()
# Pydantic model for file information
class FileInfo(BaseModel):
    filename: str
    size: int
    storage_path: str
    metadata: Dict[str, str] = {}

    class Config:
        from_attributes = True  # Allows ORM mode-like functionality
    
class CollectionInfo(BaseModel):
    name: str
    vector_size: int
    total_points: int
    creation_date: Optional[datetime] = None
    
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
        
redis_client = RedisClient(
        os.getenv("REDIS_HOST"),
        int(os.getenv("REDIS_PORT")),
        int(os.getenv("REDIS_DB"))
)

class JobStatus(BaseModel):
    job_id: str
    status: str
    last_updated: str
    details: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    error_description: Optional[str] = None
    retry_count: Optional[int] = None

qdrant_client = QdrantClient(url="http://localhost:6333")

# File Management APIs
@app.get("/organizations/{organization_id}/files", response_model=List[FileInfo])
async def list_organization_files(
    organization_id: str,
    prefix: Optional[str] = None,
    limit: int = 100
):
    """
    List all files for an organization with optional prefix filtering
    """
    bucket_name = "documents"
    prefix_path = f"{organization_id}/"
    if prefix:
        prefix_path = f"{organization_id}/{prefix}"

    try:
        files = []
        # Get the list of objects
        objects = list(minio_client.list_objects(bucket_name, prefix=prefix_path))
        
        # For debugging
        print(f"Found {len(objects)} objects")
        if objects:
            print(f"First object attributes: {dir(objects[0])}")
        
        for obj in objects:
            if len(files) >= limit:
                break
            
            try:
                # Extract object information using the correct attributes
                file_info = FileInfo(
                    filename=obj.object_name.split('/')[-1],
                    size=obj.size,
                    last_modified=obj.last_modified,
                    storage_path=f"minio://{bucket_name}/{obj.object_name}",
                    metadata={}  # Initialize empty metadata
                )
                
                files.append(file_info)
                
            except Exception as e:
                print(f"Error processing object {obj}: {e}")
                continue

        return files

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing files: {str(e)}")


@app.delete("/organizations/{organization_id}/files/{filename}")
async def delete_organization_file(
    organization_id: str,
    filename: str,
    delete_vectors: bool = True
):
    """
    Delete a specific file and optionally its associated vectors from Qdrant
    """
    try:
        bucket_name = "documents"
        object_name = f"{organization_id}/{filename}"
        
        # Delete from MinIO
        try:
            minio_client.remove_object(bucket_name, object_name)
            logger.info(f"Deleted file {object_name} from MinIO")
        except Exception as e:
            logger.error(f"Error deleting file from MinIO: {e}")
            raise HTTPException(status_code=404, detail="File not found")

        # Delete associated vectors from Qdrant if requested
        if delete_vectors:
            storage_path = f"minio://{bucket_name}/{object_name}"
            try:
                # Delete points with matching storage_path
                qdrant_client.delete(
                    collection_name=organization_id,
                    points_selector=models.FilterSelector(
                        filter=models.Filter(
                            must=[
                                models.FieldCondition(
                                    key="storage_path",
                                    match=models.MatchValue(value=storage_path),
                                ),
                            ],
                        )
                    ),
                )
                logger.info(f"Deleted vectors for {storage_path} from Qdrant")
            except Exception as e:
                logger.error(f"Error deleting vectors from Qdrant: {e}")
                # Continue since file was deleted successfully

        return {"message": "File deleted successfully"}

    except Exception as e:
        logger.error(f"Error in delete operation: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Collection Management APIs
@app.post("/collections")
async def create_collection(
    collection_name: str,
    vector_size: int = 384,
    distance: str = "COSINE"
):
    """
    Create a new collection in Qdrant
    """
    try:
        # Check if collection already exists
        collections = qdrant_client.get_collections()
        if any(collection.name == collection_name for collection in collections.collections):
            raise HTTPException(
                status_code=400,
                detail=f"Collection '{collection_name}' already exists"
            )

        # Create collection
        qdrant_client.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(
                size=vector_size,
                distance=Distance[distance],
                on_disk=True
            ),
            optimizers_config=OptimizersConfigDiff(
                indexing_threshold=20000
            )
        )

        return {
            "message": f"Collection '{collection_name}' created successfully",
            "details": {
                "name": collection_name,
                "vector_size": vector_size,
                "distance": distance
            }
        }

    except Exception as e:
        logger.error(f"Error creating collection: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/collections", response_model=List[CollectionInfo])
async def list_collections():
    """
    List all collections in Qdrant
    """
    try:
        collections = qdrant_client.get_collections()
        collection_info = []

        for collection in collections.collections:
            # Get collection info
            try:
                collection_data = qdrant_client.get_collection(collection.name)
                vectors_config = collection_data.config.params.vectors
                
                # Get collection statistics
                stats = qdrant_client.get_collection(collection.name)
                
                info = CollectionInfo(
                    name=collection.name,
                    vector_size=vectors_config.size,
                    total_points=stats.points_count,
                )
                collection_info.append(info)
            except Exception as e:
                logger.error(f"Error getting info for collection {collection.name}: {e}")
                continue

        return collection_info

    except Exception as e:
        logger.error(f"Error listing collections: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/collections/{collection_name}")
async def delete_collection(collection_name: str):
    """
    Delete a collection from Qdrant
    """
    try:
        # Check if collection exists
        collections = qdrant_client.get_collections()
        if not any(collection.name == collection_name for collection in collections.collections):
            raise HTTPException(
                status_code=404,
                detail=f"Collection '{collection_name}' not found"
            )

        # Delete collection
        qdrant_client.delete_collection(collection_name)

        return {
            "message": f"Collection '{collection_name}' deleted successfully"
        }

    except Exception as e:
        logger.error(f"Error deleting collection: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/collections/{collection_name}/stats")
async def get_collection_stats(collection_name: str):
    """
    Get detailed statistics for a specific collection
    """
    try:
        # Check if collection exists
        collections = qdrant_client.get_collections()
        if not any(collection.name == collection_name for collection in collections.collections):
            raise HTTPException(
                status_code=404,
                detail=f"Collection '{collection_name}' not found"
            )

        # Get collection info and stats
        collection_info = qdrant_client.get_collection(collection_name)
        
        return {
            "name": collection_name,
            "vector_size": collection_info.config.params.vectors.size,
            "distance": collection_info.config.params.vectors.distance,
            "total_points": collection_info.points_count,
            "segments_count": collection_info.segments_count,
            "status": collection_info.status,
            "optimization_config": {
                "indexing_threshold": collection_info.config.optimizers_config.indexing_threshold
            }
        }

    except Exception as e:
        logger.error(f"Error getting collection stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    
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

    return {
        "job_id": job_id,
        "status": "queued",
        "storage_path": storage_path
    }

@app.get("/status/{job_id}", response_model=JobStatus)
async def get_status(job_id: str):
    """
    Get detailed status information for a specific job
    """
    status_data = redis_client.get_latest_status(job_id)
    
    if not status_data:
        raise HTTPException(status_code=404, detail="Job not found")
    
    try:
        # Convert string representation of details back to dict if possible
        details = status_data.get('details')
        if details:
            try:
                details = eval(details)  # Safe since we control what goes into Redis
            except:
                details = {"raw": details}
                
        return JobStatus(
            job_id=job_id,
            status=status_data.get('status', 'UNKNOWN'),
            last_updated=status_data.get('datetime', ''),
            details=details,
            error=status_data.get('error'),
            error_description=status_data.get('error_description'),
            retry_count=int(status_data.get('retry_count', 0)) if status_data.get('retry_count') else None
        )
    except Exception as e:
        logger.error(f"Error processing status data: {e}")
        raise HTTPException(status_code=500, detail="Error processing status data")