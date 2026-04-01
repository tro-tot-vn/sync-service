import redis
import threading
import time
from loguru import logger
from config.settings import settings
from services import get_milvus_service
from workers import PostSyncWorker


def main():
    logger.info("🚀 Starting sync-service with Debezium CDC...")
    
    # Initialize Milvus
    try:
        milvus_service = get_milvus_service()
        milvus_service.initialize()
        logger.info("✅ Milvus initialized")
    except Exception as e:
        logger.error(f"❌ Failed to initialize Milvus: {e}")
        return
    
    # Create Redis client for Redis Streams
    redis_client = redis.Redis(
        host=settings.REDIS_HOST, 
        port=settings.REDIS_PORT, 
        password=settings.REDIS_PASSWORD,
        decode_responses=False  # Keep bytes for binary-safe operations
    )
    
    # Test Redis connection
    try:
        redis_client.ping()
        logger.info("✅ Redis connection successful")
    except Exception as e:
        logger.error(f"❌ Failed to connect to Redis: {e}")
        return
    
    # Create worker
    post_worker = PostSyncWorker(redis_client)
    
    # Start worker in separate thread
    logger.info("🔄 Starting CDC event consumer...")
    post_thread = threading.Thread(target=post_worker.start, daemon=True, name="PostWorker")
    
    post_thread.start()
    
    logger.info("✅ Worker started")
    logger.info("📡 Consuming CDC events from Redis Streams:")
    logger.info("   - Stream: dbserver.TroTotVN.dbo.Post")
    logger.info("   - Group: sync-service-group")
    logger.info("   - Consumer: post-worker")
    
    try:
        # Keep main thread alive
        while True:
            time.sleep(60)
            logger.debug("⏱️  Sync service heartbeat...")
    except KeyboardInterrupt:
        logger.info("👋 Shutting down sync-service...")


if __name__ == "__main__":
    main()
