from typing import Dict
from loguru import logger
from services.debezium_consumer import DebeziumConsumer
from services import get_milvus_service, get_embedding_service


class UserSyncWorker:
    """
    Worker to sync Customer changes from SQL Server (via Debezium CDC) to Milvus.
    All customers are synced (no status filtering).
    """
    
    def __init__(self, redis_client):
        self.consumer = DebeziumConsumer(
            redis_client=redis_client,
            stream_name='dbz.trotot.Customer',
            group_name='sync-service-group',
            consumer_name='user-worker'
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
        
        if operation in ['c', 'u']:  # Create or Update
            data = payload['after']
            customer_id = data['customerId']
            
            try:
                # Generate embedding
                text = self.embedding.prepare_user_text(data)
                logger.debug(f"  📝 Generating embedding for customer {customer_id}...")
                embedding = self.embedding.generate_dense_embedding(text, dim=128)
                
                # Upsert to Milvus
                logger.debug(f"  💾 Upserting customer {customer_id} to Milvus...")
                self.milvus.upsert_user(
                    user_id=customer_id,
                    embedding=embedding,
                    data=data
                )
                
                op_name = 'created' if operation == 'c' else 'updated'
                logger.info(f"✅ Customer {customer_id} {op_name} and synced")
                
            except Exception as e:
                logger.error(f"❌ Failed to sync customer {customer_id}: {e}")
                raise
        
        elif operation == 'd':  # Delete
            customer_id = payload['before']['customerId']
            
            try:
                self.milvus.delete_user(customer_id)
                logger.info(f"🗑️  Customer {customer_id} deleted from Milvus")
            except Exception as e:
                logger.error(f"❌ Failed to delete customer {customer_id}: {e}")
                raise
        
        elif operation == 'r':  # Snapshot read (skip for now)
            logger.debug(f"⏭️  Skipping snapshot read event")
