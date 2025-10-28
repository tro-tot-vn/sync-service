from typing import Dict
from datetime import datetime
from loguru import logger
from services.debezium_consumer import DebeziumConsumer
from services import get_milvus_service, get_embedding_service


class PostSyncWorker:
    """
    Worker to sync Post changes from SQL Server (via Debezium CDC) to Milvus.
    Only APPROVED posts are stored in Milvus.
    """
    
    def __init__(self, redis_client):
        self.consumer = DebeziumConsumer(
            redis_client=redis_client,
            stream_name='dbz.trotot.TroTotVN.dbo.Post',  # Real stream name from Debezium
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
        """Handle post creation"""
        post_id = after_data['postId']
        status = after_data.get('status')
        
        # Only insert if Approved (capitalized, as per SQL Server data)
        if status == 'Approved':
            self._sync_to_milvus(after_data)
            logger.info(f"✅ Post {post_id} created and synced (Approved)")
        else:
            logger.debug(f"⏭️  Post {post_id} created but not Approved (status: {status}), skipping")
    
    def _handle_update(self, before_data: Dict, after_data: Dict):
        """Handle post update with state transition logic"""
        post_id = after_data['postId']
        old_status = before_data.get('status')
        new_status = after_data.get('status')
        
        # State transition logic (status is capitalized: "Approved", "Pending", "Rejected")
        if old_status != 'Approved' and new_status == 'Approved':
            # Transition to Approved: INSERT to Milvus
            self._sync_to_milvus(after_data)
            logger.info(f"✅ Post {post_id} approved (transition: {old_status} → {new_status}), synced to Milvus")
        
        elif old_status == 'Approved' and new_status == 'Approved':
            # Still Approved: UPDATE in Milvus
            self._sync_to_milvus(after_data)
            logger.info(f"♻️  Post {post_id} updated in Milvus (still Approved)")
        
        elif old_status == 'Approved' and new_status != 'Approved':
            # Transition from Approved: DELETE from Milvus
            self.milvus.delete_post(post_id)
            logger.info(f"🗑️  Post {post_id} status changed (Approved → {new_status}), deleted from Milvus")
        
        else:
            # Not Approved before or after: do nothing
            logger.debug(f"⏭️  Post {post_id} status: {old_status} → {new_status} (not Approved), skipping")
    
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
