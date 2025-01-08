import logging
import time
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
import numpy as np
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ServiceConfig:
    base_url: str = "http://127.0.0.1:8123"
    timeout: int = 30
    max_retries: int = 3
    backoff_factor: float = 0.5
    retry_status_codes: List[int] = (408, 429, 500, 502, 503, 504)

class EmbeddingServiceClient:
    def __init__(self, config: Optional[ServiceConfig] = None):
        self.config = config or ServiceConfig()
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.backoff_factor,
            status_forcelist=self.config.retry_status_codes
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def submit_request(self, text: str, priority: str) -> Dict[str, Any]:
        """Submit text for processing with priority level."""
        try:
            response = self.session.post(
                f"{self.config.base_url}/submit",
                json={"text": text, "priority": priority},
                headers={"Content-Type": "application/json"},
                timeout=self.config.timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to submit request: {str(e)}")
            raise

    def get_status(self, request_id: str) -> Dict[str, Any]:
        """Check processing status of a request."""
        try:
            response = self.session.get(
                f"{self.config.base_url}/status/{request_id}",
                timeout=self.config.timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get status for request {request_id}: {str(e)}")
            raise

    def get_result(self, request_id: str) -> Dict[str, Any]:
        """Fetch the final result of a processed request."""
        try:
            response = self.session.get(
                f"{self.config.base_url}/result/{request_id}",
                timeout=self.config.timeout
            )
            response.raise_for_status()
            return response.json()["embedding"]
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get result for request {request_id}: {str(e)}")
            raise

    def get_embedding(self, text: str, use_cache: bool = True) -> Optional[List[float]]:
        """Get embedding for given text."""
        try:
            response = self.session.post(
                f"{self.config.base_url}/embed_sync",
                json={"text": text, "use_cache": use_cache},
                timeout=self.config.timeout
            )
            response.raise_for_status()
            embedding = response.json()["embedding"]
            logger.info(f"Embedding shape: {np.array(embedding).shape}")
            return embedding
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get embedding: {str(e)}")
            return None

    def process_with_polling(
        self, text: str, priority: str, 
        polling_interval: int = 3, 
        max_retries: int = 1000
    ) -> Dict[str, Any]:
        """Submit request and poll for results with exponential backoff."""
        response = self.submit_request(text, priority)
        request_id = response.get("request_id")
        if not request_id:
            raise ValueError("No request ID received")

        retries = 0
        while retries < max_retries:
            status_data = self.get_status(request_id)
            status = status_data.get("status")
            error = status_data.get("error")

            if error:
                raise RuntimeError(f"Processing error: {error}")
            if status == "completed":
                return self.get_result(request_id)

            time.sleep(min(polling_interval * (2 ** retries), 60))
            retries += 1

        raise TimeoutError("Maximum polling attempts exceeded")

# Example usage
def main():
    client = EmbeddingServiceClient()
    try:
        # Example 1: Get embedding
        embedding = client.get_embedding("Sample text")
        if embedding:
            logger.info("Successfully obtained embedding")

        # Example 2: Process with polling
        result = client.process_with_polling(
            "Process this text", 
            priority="medium"
        )
        logger.info(f"Processing result: {result}")

    except Exception as e:
        logger.error(f"Operation failed: {str(e)}")

if __name__ == "__main__":
    main()