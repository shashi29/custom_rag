# from fastapi import FastAPI, UploadFile, File, Form, HTTPException
# from minio_utils import MinioClient
# import os, uuid, json
# from datetime import datetime
# import tempfile
# from typing import Optional, Dict, Any, Tuple
# from pydantic import BaseModel
# from rabbitmq_utils import RabbitMQClient
# import redis
# from ocr_service import ProcessingEvent, RedisClient
# from enum import Enum
# from dataclasses import dataclass, asdict
# from ocr_service import *
# from fastapi import FastAPI, HTTPException, Query
# from typing import List, Optional, Dict, Any
# from pydantic import BaseModel
# from minio_utils import MinioClient
# from qdrant_client import QdrantClient
# from qdrant_client.http.models import Distance, VectorParams, OptimizersConfigDiff
# import logging
# from datetime import datetime
# from typing import List

# from dotenv import load_dotenv
# # Load environment variables
# load_dotenv()
# # Pydantic model for file information
# class FileInfo(BaseModel):
#     filename: str
#     size: int
#     storage_path: str
#     metadata: Dict[str, str] = {}

#     class Config:
#         from_attributes = True  # Allows ORM mode-like functionality
    
# class CollectionInfo(BaseModel):
#     name: str
#     vector_size: int
#     total_points: int
#     creation_date: Optional[datetime] = None
    
# class FileUploadRequest(BaseModel):
#     organization_id: str  
#     metadata: Optional[Dict[str, Any]] = {}
    
# app = FastAPI()

# minio_client = MinioClient(
#     endpoint="localhost:9000",
#     access_key="BVxA5YuSF5vkzXkXMym7", 
#     secret_key="jLDOAIEsfef50DK90gIPb5ucON7K1WuC93dOKa8F",
#     secure=False
# )
        
# redis_client = RedisClient(
#         os.getenv("REDIS_HOST"),
#         int(os.getenv("REDIS_PORT")),
#         int(os.getenv("REDIS_DB"))
# )

# class JobStatus(BaseModel):
#     job_id: str
#     status: str
#     last_updated: str
#     details: Optional[Dict[str, Any]] = None
#     error: Optional[str] = None
#     error_description: Optional[str] = None
#     retry_count: Optional[int] = None

# qdrant_client = QdrantClient(url="http://localhost:6333")

# # File Management APIs
# @app.get("/organizations/{organization_id}/files", response_model=List[FileInfo])
# async def list_organization_files(
#     organization_id: str,
#     prefix: Optional[str] = None,
#     limit: int = 100
# ):
#     """
#     List all files for an organization with optional prefix filtering
#     """
#     bucket_name = "documents"
#     prefix_path = f"{organization_id}/"
#     if prefix:
#         prefix_path = f"{organization_id}/{prefix}"

#     try:
#         files = []
#         # Get the list of objects
#         objects = list(minio_client.list_objects(bucket_name, prefix=prefix_path))
        
#         # For debugging
#         print(f"Found {len(objects)} objects")
#         if objects:
#             print(f"First object attributes: {dir(objects[0])}")
        
#         for obj in objects:
#             if len(files) >= limit:
#                 break
            
#             try:
#                 # Extract object information using the correct attributes
#                 file_info = FileInfo(
#                     filename=obj.object_name.split('/')[-1],
#                     size=obj.size,
#                     last_modified=obj.last_modified,
#                     storage_path=f"minio://{bucket_name}/{obj.object_name}",
#                     metadata={}  # Initialize empty metadata
#                 )
                
#                 files.append(file_info)
                
#             except Exception as e:
#                 print(f"Error processing object {obj}: {e}")
#                 continue

#         return files

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error listing files: {str(e)}")


# @app.delete("/organizations/{organization_id}/files/{filename}")
# async def delete_organization_file(
#     organization_id: str,
#     filename: str,
#     delete_vectors: bool = True
# ):
#     """
#     Delete a specific file and optionally its associated vectors from Qdrant
#     """
#     try:
#         bucket_name = "documents"
#         object_name = f"{organization_id}/{filename}"
        
#         # Delete from MinIO
#         try:
#             minio_client.remove_object(bucket_name, object_name)
#             logger.info(f"Deleted file {object_name} from MinIO")
#         except Exception as e:
#             logger.error(f"Error deleting file from MinIO: {e}")
#             raise HTTPException(status_code=404, detail="File not found")

#         # Delete associated vectors from Qdrant if requested
#         if delete_vectors:
#             storage_path = f"minio://{bucket_name}/{object_name}"
#             try:
#                 # Delete points with matching storage_path
#                 qdrant_client.delete(
#                     collection_name=organization_id,
#                     points_selector=models.FilterSelector(
#                         filter=models.Filter(
#                             must=[
#                                 models.FieldCondition(
#                                     key="storage_path",
#                                     match=models.MatchValue(value=storage_path),
#                                 ),
#                             ],
#                         )
#                     ),
#                 )
#                 logger.info(f"Deleted vectors for {storage_path} from Qdrant")
#             except Exception as e:
#                 logger.error(f"Error deleting vectors from Qdrant: {e}")
#                 # Continue since file was deleted successfully

#         return {"message": "File deleted successfully"}

#     except Exception as e:
#         logger.error(f"Error in delete operation: {e}")
#         raise HTTPException(status_code=500, detail=str(e))

# # Collection Management APIs
# @app.post("/collections")
# async def create_collection(
#     collection_name: str,
#     vector_size: int = 384,
#     distance: str = "COSINE"
# ):
#     """
#     Create a new collection in Qdrant
#     """
#     try:
#         # Check if collection already exists
#         collections = qdrant_client.get_collections()
#         if any(collection.name == collection_name for collection in collections.collections):
#             raise HTTPException(
#                 status_code=400,
#                 detail=f"Collection '{collection_name}' already exists"
#             )

#         # Create collection
#         qdrant_client.create_collection(
#             collection_name=collection_name,
#             vectors_config=VectorParams(
#                 size=vector_size,
#                 distance=Distance[distance],
#                 on_disk=True
#             ),
#             optimizers_config=OptimizersConfigDiff(
#                 indexing_threshold=20000
#             )
#         )

#         return {
#             "message": f"Collection '{collection_name}' created successfully",
#             "details": {
#                 "name": collection_name,
#                 "vector_size": vector_size,
#                 "distance": distance
#             }
#         }

#     except Exception as e:
#         logger.error(f"Error creating collection: {e}")
#         raise HTTPException(status_code=500, detail=str(e))

# @app.get("/collections", response_model=List[CollectionInfo])
# async def list_collections():
#     """
#     List all collections in Qdrant
#     """
#     try:
#         collections = qdrant_client.get_collections()
#         collection_info = []

#         for collection in collections.collections:
#             # Get collection info
#             try:
#                 collection_data = qdrant_client.get_collection(collection.name)
#                 vectors_config = collection_data.config.params.vectors
                
#                 # Get collection statistics
#                 stats = qdrant_client.get_collection(collection.name)
                
#                 info = CollectionInfo(
#                     name=collection.name,
#                     vector_size=vectors_config.size,
#                     total_points=stats.points_count,
#                 )
#                 collection_info.append(info)
#             except Exception as e:
#                 logger.error(f"Error getting info for collection {collection.name}: {e}")
#                 continue

#         return collection_info

#     except Exception as e:
#         logger.error(f"Error listing collections: {e}")
#         raise HTTPException(status_code=500, detail=str(e))

# @app.delete("/collections/{collection_name}")
# async def delete_collection(collection_name: str):
#     """
#     Delete a collection from Qdrant
#     """
#     try:
#         # Check if collection exists
#         collections = qdrant_client.get_collections()
#         if not any(collection.name == collection_name for collection in collections.collections):
#             raise HTTPException(
#                 status_code=404,
#                 detail=f"Collection '{collection_name}' not found"
#             )

#         # Delete collection
#         qdrant_client.delete_collection(collection_name)

#         return {
#             "message": f"Collection '{collection_name}' deleted successfully"
#         }

#     except Exception as e:
#         logger.error(f"Error deleting collection: {e}")
#         raise HTTPException(status_code=500, detail=str(e))

# @app.get("/collections/{collection_name}/stats")
# async def get_collection_stats(collection_name: str):
#     """
#     Get detailed statistics for a specific collection
#     """
#     try:
#         # Check if collection exists
#         collections = qdrant_client.get_collections()
#         if not any(collection.name == collection_name for collection in collections.collections):
#             raise HTTPException(
#                 status_code=404,
#                 detail=f"Collection '{collection_name}' not found"
#             )

#         # Get collection info and stats
#         collection_info = qdrant_client.get_collection(collection_name)
        
#         return {
#             "name": collection_name,
#             "vector_size": collection_info.config.params.vectors.size,
#             "distance": collection_info.config.params.vectors.distance,
#             "total_points": collection_info.points_count,
#             "segments_count": collection_info.segments_count,
#             "status": collection_info.status,
#             "optimization_config": {
#                 "indexing_threshold": collection_info.config.optimizers_config.indexing_threshold
#             }
#         }

#     except Exception as e:
#         logger.error(f"Error getting collection stats: {e}")
#         raise HTTPException(status_code=500, detail=str(e))

    
# @app.post("/submit-job")
# async def submit_job(
#     file: UploadFile = File(...),
#     organization_id: str = Form(...),
#     metadata: str = Form("{}")
# ):
#     temp_file = None
#     # try:
#     temp_file = tempfile.NamedTemporaryFile(delete=False)
#     file_content = await file.read()
#     temp_file.write(file_content)
#     temp_file.close()

#     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#     object_name = f"{organization_id}/{timestamp}_{file.filename}"
#     bucket_name = "documents"

#     minio_client.upload_file(bucket_name, object_name, temp_file.name)
#     storage_path = f"minio://{bucket_name}/{object_name}"

#     job_id = str(uuid.uuid4())
#     message = {
#         "job_id": job_id,
#         "collection_name": organization_id,
#         "storage_path": storage_path,
#         "metadata": json.loads(metadata)
#     }
#     rabbitmq_client = RabbitMQClient(
#             host="localhost",
#             queue="custom_rag",
#             user="cwRI82uX5HyT",
#             password="9j4u6PluofN5"
#     )

#     rabbitmq_client.send_message(message, 0)

#     return {
#         "job_id": job_id,
#         "status": "queued",
#         "storage_path": storage_path
#     }

# @app.get("/status/{job_id}", response_model=JobStatus)
# async def get_status(job_id: str):
#     """
#     Get detailed status information for a specific job
#     """
#     status_data = redis_client.get_latest_status(job_id)
    
#     if not status_data:
#         raise HTTPException(status_code=404, detail="Job not found")
    
#     try:
#         # Convert string representation of details back to dict if possible
#         details = status_data.get('details')
#         if details:
#             try:
#                 details = eval(details)  # Safe since we control what goes into Redis
#             except:
#                 details = {"raw": details}
                
#         return JobStatus(
#             job_id=job_id,
#             status=status_data.get('status', 'UNKNOWN'),
#             last_updated=status_data.get('datetime', ''),
#             details=details,
#             error=status_data.get('error'),
#             error_description=status_data.get('error_description'),
#             retry_count=int(status_data.get('retry_count', 0)) if status_data.get('retry_count') else None
#         )
#     except Exception as e:
#         logger.error(f"Error processing status data: {e}")
#         raise HTTPException(status_code=500, detail="Error processing status data")


# from fastapi import FastAPI, Form, Depends, UploadFile, File, HTTPException
# from sqlmodel import SQLModel, Field, Column, VARCHAR, TEXT, create_engine, Session
# from datetime import datetime
# from dotenv import load_dotenv
# import os
# from qdrant_client import QdrantClient
# import openai
# from langchain.llms import OpenAI
# from langchain.chains import ConversationChain
# from langchain.memory import ConversationBufferMemory
# from langchain.vectorstores import Qdrant
# from langchain.embeddings import OpenAIEmbeddings
# from langchain.schema import Document
# from typing import Optional, List, Dict, Any
# import uuid
# import json
# from sentence_transformers import SentenceTransformer
# # Load environment variables
# load_dotenv()

# # Define SQLModel classes
# class ChatHistory(SQLModel, table=True):
#     id: int = Field(default=None, primary_key=True)
#     session_id: str = Field(sa_column=Column(VARCHAR(255), nullable=False))
#     sender: str = Field(sa_column=Column(VARCHAR(50), nullable=False))
#     message: str = Field(sa_column=Column(TEXT, nullable=False))
#     collection_name: str = Field(sa_column=Column(VARCHAR(255), nullable=False))
#     timestamp: datetime = Field(default_factory=datetime.utcnow)

# # Set up database engine and session
# DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
# engine = create_engine(DATABASE_URL, echo=True)

# def get_session():
#     with Session(engine) as session:
#         yield session

# # Initialize FastAPI app
# app = FastAPI()

# # Initialize Qdrant and OpenAI clients
# qdrant_client = QdrantClient(url=os.getenv("QDRANT_URL"))
# openai.api_key = os.getenv("OPENAI_API_KEY")
# embeddings = SentenceTransformer("all-MiniLM-L6-v2")

# # Create table if not exists
# SQLModel.metadata.create_all(engine)

# # Implement the /chat endpoint
# @app.post("/chat")
# async def chat(
#     collection_name: str = Form(...),
#     message: str = Form(...),
#     session_id: Optional[str] = Form(None),
#     db: Session = Depends(get_session)
# ):
#     # Validate collection exists in Qdrant
#     collections = qdrant_client.get_collections()
#     if not any(collection.name == collection_name for collection in collections.collections):
#         raise HTTPException(
#             status_code=404,
#             detail=f"Collection '{collection_name}' not found"
#         )

#     # Create a session ID if not provided
#     if not session_id:
#         session_id = str(uuid.uuid4())

#     # Generate embedding for the user's message
#     embedding = embeddings.encode([message])[0]
#     # Search Qdrant for top N similar vectors
#     search_result = qdrant_client.search(
#         collection_name=collection_name,
#         query_vector=embedding,
#         limit=5
#     )

#     # Retrieve relevant text and metadata from search results
#     context = ""
#     for hit in search_result:
#         payload = hit.payload
#         context += f"Text: {payload.get('text')}\n"
#         context += f"Page Number: {payload.get('page_number')}\n"
#         context += f"Storage Path: {payload.get('storage_path')}\n"
#         context += f"Metadata: {payload.get('metadata')}\n\n"

#     # Use LangChain to manage conversation memory
#     memory = ConversationBufferMemory()
#     conversation = ConversationChain(
#         llm=OpenAI(),
#         memory=memory
#     )

#     # Generate response from OpenAI's chat model
#     response = conversation.predict(input=f"Context: {context}\nUser: {message}")

#     # Save chat messages to the database
#     user_message = ChatHistory(
#         session_id=session_id,
#         sender="user",
#         message=message,
#         collection_name=collection_name
#     )
#     save_chat_message(db, user_message)

#     assistant_message = ChatHistory(
#         session_id=session_id,
#         sender="assistant",
#         message=response,
#         collection_name=collection_name
#     )
#     save_chat_message(db, assistant_message)

#     return {
#         "session_id": session_id,
#         "response": response
#     }

# # Define function to save chat messages
# def save_chat_message(session: Session, message: ChatHistory):
#     session.add(message)
#     session.commit()
#     session.refresh(message)

# # Create GET endpoint for chat history
# @app.get("/chat/history")
# async def get_chat_history(
#     session_id: str,
#     collection_name: str,
#     db: Session = Depends(get_session)
# ):
#     messages = db.query(ChatHistory).filter(
#         ChatHistory.session_id == session_id,
#         ChatHistory.collection_name == collection_name
#     ).order_by(ChatHistory.timestamp).all()
#     return messages
from qdrant_client.http.models import PointStruct, Filter, FieldCondition, MatchValue, UpdateStatus
from qdrant_client.http import models
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct, VectorParams, Distance, UpdateStatus, OptimizersConfigDiff

from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Depends
from pydantic import BaseModel
from sqlmodel import SQLModel, Field, Column, VARCHAR, TEXT, create_engine, Session
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams, OptimizersConfigDiff
from sentence_transformers import SentenceTransformer
from langchain.llms import OpenAI
from langchain.chains import ConversationChain
from langchain.memory import ConversationBufferMemory
from minio_utils import MinioClient
from rabbitmq_utils import RabbitMQClient
from ocr_service import RedisClient
from typing import List, Optional, Dict, Any
from datetime import datetime
import os, uuid, json, tempfile, logging
from dotenv import load_dotenv

load_dotenv()

# Models
class FileInfo(BaseModel):
    filename: str
    size: int
    storage_path: str
    metadata: Dict[str, str] = {}
    class Config:
        from_attributes = True

class CollectionInfo(BaseModel):
    name: str
    vector_size: int
    total_points: int
    creation_date: Optional[datetime] = None

class JobStatus(BaseModel):
    job_id: str
    status: str
    last_updated: str
    details: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    error_description: Optional[str] = None
    retry_count: Optional[int] = None

class ChatHistory(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    session_id: str = Field(sa_column=Column(VARCHAR(255), nullable=False))
    sender: str = Field(sa_column=Column(VARCHAR(50), nullable=False))
    message: str = Field(sa_column=Column(TEXT, nullable=False))
    collection_name: str = Field(sa_column=Column(VARCHAR(255), nullable=False))
    timestamp: datetime = Field(default_factory=datetime.utcnow)

# Initialize clients and configurations
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
qdrant_client = QdrantClient(url="http://localhost:6333")
embeddings = SentenceTransformer("all-MiniLM-L6-v2")

# Database setup
DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(DATABASE_URL)
SQLModel.metadata.create_all(engine)

def get_session():
    with Session(engine) as session:
        yield session

# File Management APIs
@app.get("/organizations/{organization_id}/files", response_model=List[FileInfo])
async def list_organization_files(
    organization_id: str,
    prefix: Optional[str] = None,
    limit: int = 100
):
    bucket_name = "documents"
    prefix_path = f"{organization_id}/{prefix if prefix else ''}"
    try:
        files = []
        objects = list(minio_client.list_objects(bucket_name, prefix=prefix_path))
        for obj in objects[:limit]:
            files.append(FileInfo(
                filename=obj.object_name.split('/')[-1],
                size=obj.size,
                storage_path=f"minio://{bucket_name}/{obj.object_name}",
                metadata={}
            ))
        return files
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing files: {str(e)}")

@app.delete("/organizations/{organization_id}/files/{filename}")
async def delete_organization_file(
    organization_id: str,
    filename: str,
    delete_vectors: bool = True
):
    bucket_name = "documents"
    object_name = f"{organization_id}/{filename}"
    try:
        minio_client.remove_object(bucket_name, object_name)
        if delete_vectors:
            storage_path = f"minio://{bucket_name}/{object_name}"
            qdrant_client.delete(
                collection_name=organization_id,
                points_selector=models.FilterSelector(
                    filter=models.Filter(
                        must=[models.FieldCondition(
                            key="storage_path",
                            match=models.MatchValue(value=storage_path)
                        )]
                    )
                )
            )
        return {"message": "File deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Collection Management APIs
@app.post("/collections")
async def create_collection(
    collection_name: str,
    vector_size: int = 384,
    distance: str = "COSINE"
):
    try:
        collections = qdrant_client.get_collections()
        if any(collection.name == collection_name for collection in collections.collections):
            raise HTTPException(status_code=400, detail=f"Collection '{collection_name}' already exists")
        
        qdrant_client.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(size=vector_size, distance=Distance[distance], on_disk=True),
            optimizers_config=OptimizersConfigDiff(indexing_threshold=20000)
        )
        return {"message": f"Collection '{collection_name}' created successfully"}
    except Exception as e:
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
        print(collection_info)
        
        return {
            "name": collection_name
        }

    except Exception as e:
        # logger.error(f"Error getting collection stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/submit-job")
async def submit_job(
    file: UploadFile = File(...),
    organization_id: str = Form(...),
    metadata: str = Form("{}")
):
    try:
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
    finally:
        if temp_file:
            os.unlink(temp_file.name)

# Chat APIs
@app.post("/chat")
async def chat(
    collection_name: str = Form(...),
    message: str = Form(...),
    session_id: Optional[str] = Form(None),
    db: Session = Depends(get_session)
):
    collections = qdrant_client.get_collections()
    if not any(collection.name == collection_name for collection in collections.collections):
        raise HTTPException(status_code=404, detail=f"Collection '{collection_name}' not found")

    session_id = session_id or str(uuid.uuid4())
    embedding = embeddings.encode([message])[0]
    
    search_result = qdrant_client.search(
        collection_name=collection_name,
        query_vector=embedding,
        limit=5
    )

    context = "\n\n".join([
        f"Text: {hit.payload.get('text')}\n"
        f"Page Number: {hit.payload.get('page_number')}\n"
        f"Storage Path: {hit.payload.get('storage_path')}\n"
        f"Metadata: {hit.payload.get('metadata')}"
        for hit in search_result
    ])

    memory = ConversationBufferMemory()
    conversation = ConversationChain(llm=OpenAI(), memory=memory)
    response = conversation.predict(input=f"Context: {context}\nUser: {message}")

    for msg in [
        ChatHistory(session_id=session_id, sender="user", message=message, collection_name=collection_name),
        ChatHistory(session_id=session_id, sender="assistant", message=response, collection_name=collection_name)
    ]:
        db.add(msg)
    db.commit()

    return {"session_id": session_id, "response": response}

@app.get("/chat/history")
async def get_chat_history(
    # session_id: str,
    collection_name: str,
    db: Session = Depends(get_session)
):
    return db.query(ChatHistory).filter(
        # ChatHistory.session_id == session_id,
        ChatHistory.collection_name == collection_name
    ).order_by(ChatHistory.timestamp).all()
    
@app.get("/chat/sessions")
async def get_chat_sessions(
    collection_name: str,
    db: Session = Depends(get_session)
):
    """
    Get all unique chat sessions for a collection
    """
    try:
        # Query unique session IDs and their latest timestamp
        sessions = db.query(
            ChatHistory.session_id,
            db.func.max(ChatHistory.timestamp).label('last_message'),
            db.func.count(ChatHistory.id).label('message_count')
        ).filter(
            ChatHistory.collection_name == collection_name
        ).group_by(
            ChatHistory.session_id
        ).order_by(
            db.func.max(ChatHistory.timestamp).desc()
        ).all()

        return [{
            "session_id": session.session_id,
            "last_message": session.last_message,
            "message_count": session.message_count
        } for session in sessions]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching chat sessions: {str(e)}")

@app.get("/chat/history/search")
async def search_chat_history(
    collection_name: str,
    query: str,
    db: Session = Depends(get_session),
    limit: int = 10,
    offset: int = 0
):
    """
    Search through chat history for specific messages
    """
    try:
        # Search through messages using ILIKE for case-insensitive search
        messages = db.query(ChatHistory).filter(
            ChatHistory.collection_name == collection_name,
            ChatHistory.message.ilike(f"%{query}%")
        ).order_by(
            ChatHistory.timestamp.desc()
        ).offset(offset).limit(limit).all()

        total_count = db.query(ChatHistory).filter(
            ChatHistory.collection_name == collection_name,
            ChatHistory.message.ilike(f"%{query}%")
        ).count()

        return {
            "messages": messages,
            "total_count": total_count,
            "offset": offset,
            "limit": limit
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching chat history: {str(e)}")

@app.delete("/chat/history/{session_id}")
async def delete_chat_history(
    session_id: str,
    collection_name: str,
    db: Session = Depends(get_session)
):
    """
    Delete all messages in a chat session
    """
    try:
        # Delete all messages for the session
        deleted_count = db.query(ChatHistory).filter(
            ChatHistory.session_id == session_id,
            ChatHistory.collection_name == collection_name
        ).delete()

        db.commit()

        if deleted_count == 0:
            raise HTTPException(status_code=404, detail="Chat session not found")

        return {
            "message": f"Successfully deleted chat session with {deleted_count} messages",
            "deleted_count": deleted_count
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Error deleting chat history: {str(e)}")

@app.get("/chat/history/export")
async def export_chat_history(
    session_id: str,
    collection_name: str,
    format: str = "json",
    db: Session = Depends(get_session)
):
    """
    Export chat history in various formats
    """
    try:
        messages = db.query(ChatHistory).filter(
            ChatHistory.session_id == session_id,
            ChatHistory.collection_name == collection_name
        ).order_by(ChatHistory.timestamp).all()

        if not messages:
            raise HTTPException(status_code=404, detail="Chat session not found")

        if format.lower() == "json":
            return {
                "session_id": session_id,
                "collection_name": collection_name,
                "messages": [
                    {
                        "sender": msg.sender,
                        "message": msg.message,
                        "timestamp": msg.timestamp.isoformat()
                    } for msg in messages
                ]
            }
        elif format.lower() == "text":
            chat_text = "\n\n".join([
                f"[{msg.timestamp.isoformat()}] {msg.sender}:\n{msg.message}"
                for msg in messages
            ])
            return PlainTextResponse(chat_text)
        else:
            raise HTTPException(status_code=400, detail="Unsupported export format")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error exporting chat history: {str(e)}")

@app.get("/chat/stats")
async def get_chat_stats(
    collection_name: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    db: Session = Depends(get_session)
):
    """
    Get statistics about chat usage
    """
    try:
        query = db.query(ChatHistory).filter(ChatHistory.collection_name == collection_name)
        
        if start_date:
            query = query.filter(ChatHistory.timestamp >= start_date)
        if end_date:
            query = query.filter(ChatHistory.timestamp <= end_date)

        total_messages = query.count()
        unique_sessions = query.distinct(ChatHistory.session_id).count()
        
        # Get message counts by sender type
        message_counts = db.query(
            ChatHistory.sender,
            db.func.count(ChatHistory.id).label('count')
        ).filter(
            ChatHistory.collection_name == collection_name
        ).group_by(ChatHistory.sender).all()

        # Calculate average messages per session
        avg_messages_per_session = total_messages / unique_sessions if unique_sessions > 0 else 0

        return {
            "total_messages": total_messages,
            "unique_sessions": unique_sessions,
            "avg_messages_per_session": round(avg_messages_per_session, 2),
            "message_counts_by_sender": {
                sender: count for sender, count in message_counts
            },
            "date_range": {
                "start": start_date.isoformat() if start_date else None,
                "end": end_date.isoformat() if end_date else None
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting chat statistics: {str(e)}")