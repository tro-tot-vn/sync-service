from typing import Dict
from datetime import datetime
import socket
import time
import uuid
from loguru import logger
from services.debezium_consumer import DebeziumConsumer
from services import get_milvus_service, get_embedding_service


class PostSyncWorker:
    """
    Worker to sync Post changes from SQL Server (via Debezium CDC) to Milvus.
    ALL posts (Approved/Pending/Rejected/Hidden) are stored with status field.
    """
    
    def __init__(self, redis_client):
        hostname = socket.gethostname()
        unique_id = str(uuid.uuid4())[:8]  # Short UUID (8 chars)
        self.consumer_name = f'post-worker-{hostname}-{unique_id}'
        
        self.consumer = DebeziumConsumer(
            redis_client=redis_client,
            stream_name='dbz.trotot.Post',
            group_name='sync-service-group',
            consumer_name=self.consumer_name
        )
        self.milvus = get_milvus_service()
        self.embedding = get_embedding_service()
        
        # Stats tracking
        self.stats = {'created': 0, 'updated': 0, 'deleted': 0, 'errors': 0}
        self.last_log_time = time.time()
    
    def start(self):
        """Start consuming CDC events"""
        logger.info(f"🚀 Worker '{self.consumer_name}' started")
        self.consumer.consume(handler=self._handle_event)
    
    def _log_stats(self, force=False):
        """Log statistics summary periodically"""
        current_time = time.time()
        elapsed = current_time - self.last_log_time
        total_events = sum(self.stats.values())
        
        # Log every 60s or every 100 events, or if forced
        if force or elapsed >= 60 or total_events >= 100:
            if elapsed > 0:
                rate = (self.stats['created'] + self.stats['updated'] + self.stats['deleted']) / elapsed
                logger.info(
                    f"📊 [{self.consumer_name}] Stats: "
                    f"{self.stats['created']} created, {self.stats['updated']} updated, "
                    f"{self.stats['deleted']} deleted, {self.stats['errors']} errors | "
                    f"Rate: {rate:.1f} posts/s"
                )
            # Reset stats
            self.stats = {'created': 0, 'updated': 0, 'deleted': 0, 'errors': 0}
            self.last_log_time = current_time
    
    def _handle_event(self, operation: str, event: Dict):
        """
        Handle CDC event based on operation type.
        
        Args:
            operation: 'c' (create), 'u' (update), 'd' (delete), 'r' (snapshot read)
            event: Debezium CDC event payload
        """
        payload = event['payload']
        
        if operation == 'c':  # Create
            self._handle_create(payload['after'])
        elif operation == 'u':  # Update
            self._handle_update(payload['before'], payload['after'])
        elif operation == 'd':  # Delete
            self._handle_delete(payload['before'])
        elif operation == 'r':  # Snapshot read (skip for now)
            logger.debug(f"⏭️  Skipping snapshot read event")
    
    def _handle_create(self, after_data: Dict):
        """Handle post creation - sync ALL posts regardless of status"""
        self._sync_to_milvus(after_data)
        self.stats['created'] += 1
        self._log_stats()
    
    def _handle_update(self, before_data: Dict, after_data: Dict):
        """Handle post update - sync ALL status changes"""
        self._sync_to_milvus(after_data)
        self.stats['updated'] += 1
        self._log_stats()
    
    def _handle_delete(self, before_data: Dict):
        """Handle post deletion"""
        post_id = before_data['postId']
        self.milvus.delete_post(post_id)
        self.stats['deleted'] += 1
        self._log_stats()
    
    def _sync_to_milvus(self, data: Dict):
        """Sync post data to Milvus (generate embedding and upsert)"""
        post_id = data['postId']
        
        try:
            # 1. Prepare text for embedding
            text = self.embedding.prepare_post_text(data)
            
            # 2. Generate dense embedding (128-dim BGE-M3)
            dense_vec = self.embedding.generate_dense_embedding(text, dim=128)
            
            # 3. Convert date strings to datetime if needed
            if isinstance(data.get('createdAt'), str):
                data['createdAt'] = datetime.fromisoformat(data['createdAt'].replace('Z', '+00:00'))
            if isinstance(data.get('extendedAt'), str):
                data['extendedAt'] = datetime.fromisoformat(data['extendedAt'].replace('Z', '+00:00'))
            
            # 4. Upsert to Milvus (BM25 sparse vectors auto-generated)
            self.milvus.upsert_post(
                post_id=post_id,
                dense_vec=dense_vec,
                data=data
            )
            
        except Exception as e:
            logger.error(f"❌ Failed to sync post {post_id} to Milvus: {e}")
            self.stats['errors'] += 1
            raise
