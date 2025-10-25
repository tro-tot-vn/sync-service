import redis
import json
import time
from loguru import logger
from services import get_embedding_service, get_milvus_service
from config.settings import settings

class UserSyncWorker:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            decode_responses=True
        )
        self.embedding_service = get_embedding_service()
        self.milvus_service = get_milvus_service()
        self.queue_name = "bullmq:user-sync:wait"
    
    def process_job(self, job_data: dict):
        """Process a single sync job"""
        operation = job_data.get("operation")
        customer_id = job_data.get("customerId")
        data = job_data.get("data")
        
        logger.info(f"🔄 Processing {operation} for user {customer_id}")
        
        try:
            if operation in ["insert", "update"]:
                self._handle_upsert(customer_id, data)
            elif operation == "delete":
                self._handle_delete(customer_id)
            else:
                raise ValueError(f"Unknown operation: {operation}")
            
            logger.info(f"✅ Completed {operation} for user {customer_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to process user {customer_id}: {e}")
            raise
    
    def _handle_upsert(self, user_id: int, data: dict):
        """Handle insert/update"""
        # 1. Prepare text
        text = self.embedding_service.prepare_user_text(data)
        
        # 2. Generate user embedding
        logger.info(f"  📝 Generating user embedding (128-dim)...")
        embedding = self.embedding_service.generate_dense_embedding(text, dim=128)
        
        # 3. Upsert to Milvus
        logger.info(f"  💾 Upserting to Milvus...")
        self.milvus_service.upsert_user(
            user_id=user_id,
            embedding=embedding,
            data=data
        )
    
    def _handle_delete(self, user_id: int):
        """Handle delete"""
        logger.info(f"  🗑️  Deleting from Milvus...")
        self.milvus_service.delete_user(user_id)
    
    def run(self):
        """Main worker loop"""
        logger.info("🚀 User sync worker started")
        
        while True:
            try:
                # Pop job from Redis queue
                result = self.redis_client.blpop("user-sync-simple", timeout=5)
                
                if result:
                    _, job_json = result
                    job_data = json.loads(job_json)
                    self.process_job(job_data)
                
            except KeyboardInterrupt:
                logger.info("👋 Shutting down worker...")
                break
            except Exception as e:
                logger.error(f"Worker error: {e}")
                time.sleep(5)


