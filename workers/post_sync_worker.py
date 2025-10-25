import redis
import json
import time
from datetime import datetime
from loguru import logger
from services import get_embedding_service, get_milvus_service
from config.settings import settings

class PostSyncWorker:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            decode_responses=True
        )
        self.embedding_service = get_embedding_service()
        self.milvus_service = get_milvus_service()
        self.queue_name = "bullmq:post-sync:wait"
    
    def process_job(self, job_data: dict):
        """Process a single sync job"""
        operation = job_data.get("operation")
        post_id = job_data.get("postId")
        data = job_data.get("data")
        
        logger.info(f"🔄 Processing {operation} for post {post_id}")
        
        try:
            if operation in ["insert", "update"]:
                self._handle_upsert(post_id, data)
            elif operation == "delete":
                self._handle_delete(post_id)
            else:
                raise ValueError(f"Unknown operation: {operation}")
            
            logger.info(f"✅ Completed {operation} for post {post_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to process post {post_id}: {e}")
            raise
    
    def _handle_upsert(self, post_id: int, data: dict):
        """Handle insert/update"""
        # 1. Prepare text for dense embedding
        text = self.embedding_service.prepare_post_text(data)
        
        # 2. Generate dense vector (BGE-M3 128-dim)
        logger.info(f"  📝 Generating dense embedding (128-dim)...")
        dense_vec = self.embedding_service.generate_dense_embedding(text, dim=128)
        
        # 3. Convert date strings to datetime if needed
        if isinstance(data.get('createdAt'), str):
            data['createdAt'] = datetime.fromisoformat(data['createdAt'].replace('Z', '+00:00'))
        if isinstance(data.get('extendedAt'), str):
            data['extendedAt'] = datetime.fromisoformat(data['extendedAt'].replace('Z', '+00:00'))
        
        # 4. Upsert to Milvus (Milvus auto-generates 3 sparse vectors from text fields)
        logger.info(f"  💾 Upserting to Milvus...")
        self.milvus_service.upsert_post(
            post_id=post_id,
            dense_vec=dense_vec,
            data=data
        )
    
    def _handle_delete(self, post_id: int):
        """Handle delete"""
        logger.info(f"  🗑️  Deleting from Milvus...")
        self.milvus_service.delete_post(post_id)
    
    def run(self):
        """Main worker loop"""
        logger.info("🚀 Post sync worker started")
        
        while True:
            try:
                # Pop job from Redis queue
                # Using simple list-based queue (compatible with BullMQ)
                result = self.redis_client.blpop("post-sync-simple", timeout=5)
                
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


