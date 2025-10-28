"""
Drop existing collections to recreate with correct config
Run this if you need to reset Milvus collections
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pymilvus import connections, utility
from loguru import logger
from config.settings import settings

def drop_collections():
    """Drop all collections"""
    logger.info("🔄 Connecting to Milvus...")
    
    connections.connect(
        alias="default",
        host=settings.MILVUS_HOST,
        port=str(settings.MILVUS_PORT)
    )
    
    logger.info("📋 Listing existing collections...")
    collections = utility.list_collections()
    logger.info(f"Found collections: {collections}")
    
    if not collections:
        logger.info("✅ No collections to drop")
        return
    
    # Drop each collection
    for collection_name in collections:
        logger.info(f"🗑️  Dropping collection: {collection_name}")
        utility.drop_collection(collection_name)
        logger.info(f"✅ Dropped: {collection_name}")
    
    logger.info("✅ All collections dropped successfully")
    logger.info("🔄 Now restart sync-service to recreate collections with correct config")

if __name__ == "__main__":
    drop_collections()

