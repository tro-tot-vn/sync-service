import redis
import json
from typing import Dict, Callable
from loguru import logger


class DebeziumConsumer:
    """
    Consumer for Debezium CDC events from Redis Streams.
    Handles consumer group creation, message consumption, and acknowledgment.
    """
    
    def __init__(self, redis_client: redis.Redis, stream_name: str, group_name: str, consumer_name: str):
        self.redis = redis_client
        self.stream_name = stream_name
        self.group_name = group_name
        self.consumer_name = consumer_name
        self._create_consumer_group()
    
    def _create_consumer_group(self):
        """Create consumer group if it doesn't exist"""
        try:
            self.redis.xgroup_create(self.stream_name, self.group_name, id='0', mkstream=True)
            logger.info(f"✅ Created consumer group '{self.group_name}' for stream '{self.stream_name}'")
        except redis.ResponseError as e:
            if 'BUSYGROUP' in str(e):
                logger.info(f"ℹ️  Consumer group '{self.group_name}' already exists")
            else:
                raise
    
    def consume(self, handler: Callable[[str, Dict], None], batch_size: int = 10):
        """
        Start consuming messages from Redis Stream.
        
        Args:
            handler: Function to handle each CDC event (operation, event_data)
            batch_size: Number of messages to read per batch
        """
        logger.info(f"🚀 Starting consumer '{self.consumer_name}' for stream '{self.stream_name}'")
        
        while True:
            try:
                # Read messages from stream
                messages = self.redis.xreadgroup(
                    self.group_name,
                    self.consumer_name,
                    {self.stream_name: '>'},
                    count=batch_size,
                    block=5000  # Block for 5 seconds
                )
                
                if not messages:
                    continue
                else: 
                    logger.info(f"🔄 Found {len(messages)} messages in stream '{self.stream_name}'")
                    
                for stream_name, stream_messages in messages:
                    for message_id, fields in stream_messages:
                        try:
                            # Parse CDC event
                            event = self._parse_cdc_event(fields)
                            operation = event['payload']['op']  # c=create, u=update, d=delete, r=read(snapshot)
                            
                            # Handle event
                            handler(operation, event)
                            
                            # Acknowledge message
                            self.redis.xack(self.stream_name, self.group_name, message_id)
                            
                        except Exception as e:
                            logger.error(f"❌ Error processing message {message_id}: {e}")
                            # Note: Message is NOT acknowledged, will be redelivered
            
            except Exception as e:
                logger.error(f"❌ Error consuming from stream: {e}")
                import time
                time.sleep(5)  # Wait before retry
    
    def _parse_cdc_event(self, fields: Dict) -> Dict:
        """Parse Debezium CDC event from Redis Stream message"""
        # Debezium sends event in 'value' field (with 'key' field for primary key)
        value_bytes = fields.get(b'value') or fields.get('value')
        if isinstance(value_bytes, bytes):
            value_str = value_bytes.decode('utf-8')
        else:
            value_str = value_bytes
        
        return json.loads(value_str)

