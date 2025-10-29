from typing import Dict
from datetime import datetime
from loguru import logger
from services.debezium_consumer import DebeziumConsumer
from services import get_milvus_service, get_embedding_service


class PostSyncWorker:
    """
    Worker to sync Post changes from SQL Server (via Debezium CDC) to Milvus.
    ALL posts (Approved/Pending/Rejected/Hidden) are stored with status field.
    """
    
    def __init__(self, redis_client):
        self.consumer = DebeziumConsumer(
            redis_client=redis_client,
            stream_name='dbserver.TroTotVN.dbo.Post',  # Debezium Redis sink stream name
            group_name='sync-service-group',
            consumer_name='post-worker'
        )
        self.milvus = get_milvus_service()
        self.embedding = get_embedding_service()
    
    def start(self):
        """Start consuming CDC events"""
        self.consumer.consume(handler=self._handle_event)
    
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
        post_id = after_data['postId']
        status = after_data.get('status', 'Unknown')
        
        self._sync_to_milvus(after_data)
        logger.info(f"✅ Post {post_id} created and synced (status: {status})")
    
    def _handle_update(self, before_data: Dict, after_data: Dict):
        """Handle post update - sync ALL status changes"""
        post_id = after_data['postId']
        old_status = before_data.get('status', 'Unknown')
        new_status = after_data.get('status', 'Unknown')
        
        # Always upsert to Milvus with new data (including status changes)
        self._sync_to_milvus(after_data)
        
        if old_status != new_status:
            logger.info(f"♻️  Post {post_id} updated (status: {old_status} → {new_status})")
        else:
            logger.info(f"♻️  Post {post_id} updated (status: {new_status})")
    
    def _handle_delete(self, before_data: Dict):
        """Handle post deletion"""
        post_id = before_data['postId']
        # Always try to delete (idempotent operation)
        self.milvus.delete_post(post_id)
        logger.info(f"🗑️  Post {post_id} deleted from database, removed from Milvus")
    
    def _sync_to_milvus(self, data: Dict):
        """Sync post data to Milvus (generate embedding and upsert)"""
        post_id = data['postId']
        
        try:
            # 1. Prepare text for embedding
            text = self.embedding.prepare_post_text(data)
            
            # 2. Generate dense embedding (128-dim BGE-M3)
            logger.debug(f"  📝 Generating embedding for post {post_id}...")
            dense_vec = self.embedding.generate_dense_embedding(text, dim=128)
            
            # 3. Convert date strings to datetime if needed
            if isinstance(data.get('createdAt'), str):
                data['createdAt'] = datetime.fromisoformat(data['createdAt'].replace('Z', '+00:00'))
            if isinstance(data.get('extendedAt'), str):
                data['extendedAt'] = datetime.fromisoformat(data['extendedAt'].replace('Z', '+00:00'))
            
            # 4. Upsert to Milvus (BM25 sparse vectors auto-generated)
            logger.debug(f"  💾 Upserting post {post_id} to Milvus...")
            self.milvus.upsert_post(
                post_id=post_id,
                dense_vec=dense_vec,
                data=data
            )
            
        except Exception as e:
            logger.error(f"❌ Failed to sync post {post_id} to Milvus: {e}")
            raise
