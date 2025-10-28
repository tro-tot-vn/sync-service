#!/usr/bin/env python3
"""
Cleanup script to remove Customer sync infrastructure:
1. Drop 'users' collection from Milvus
2. Delete Customer-related Redis Streams
"""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import redis
from pymilvus import connections, utility
from loguru import logger
from config.settings import settings

def main():
    logger.info("🧹 Starting Customer sync cleanup...")
    
    # 1. Connect to Milvus and drop users collection
    try:
        connections.connect(
            alias="default",
            host=settings.MILVUS_HOST,
            port=str(settings.MILVUS_PORT)
        )
        logger.info(f"✅ Connected to Milvus at {settings.MILVUS_HOST}:{settings.MILVUS_PORT}")
        
        if utility.has_collection("users"):
            utility.drop_collection("users")
            logger.info("✅ Dropped 'users' collection from Milvus")
        else:
            logger.info("ℹ️  'users' collection does not exist, skipping")
    except Exception as e:
        logger.error(f"❌ Failed to drop users collection: {e}")
    
    # 2. Connect to Redis and delete Customer streams
    try:
        redis_client = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            decode_responses=False
        )
        redis_client.ping()
        logger.info(f"✅ Connected to Redis at {settings.REDIS_HOST}:{settings.REDIS_PORT}")
        
        # List of potential Customer-related streams
        customer_streams = [
            b'dbz.trotot.TroTotVN.dbo.Customer',
            b'dbz.trotot.Customer',
        ]
        
        for stream_name in customer_streams:
            try:
                if redis_client.exists(stream_name):
                    redis_client.delete(stream_name)
                    logger.info(f"✅ Deleted Redis stream: {stream_name.decode()}")
                else:
                    logger.info(f"ℹ️  Stream {stream_name.decode()} does not exist, skipping")
            except Exception as e:
                logger.error(f"❌ Failed to delete stream {stream_name.decode()}: {e}")
        
        # Also delete schema history if it exists
        schema_history_key = b'schema-history:dbz.trotot'
        if redis_client.exists(schema_history_key):
            logger.info(f"ℹ️  Schema history key exists: {schema_history_key.decode()}")
            logger.info("   (Not deleting - shared by Post CDC)")
        
    except Exception as e:
        logger.error(f"❌ Failed to clean up Redis streams: {e}")
    
    logger.info("🎉 Cleanup complete!")

if __name__ == "__main__":
    main()

