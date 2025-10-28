import redis
import threading
import time
from loguru import logger
from config.settings import settings
from services import get_milvus_service
from workers import PostSyncWorker, UserSyncWorker


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
        decode_responses=False  # Keep bytes for binary-safe operations
    )
    
    # Test Redis connection
    try:
        redis_client.ping()
        logger.info("✅ Redis connection successful")
    except Exception as e:
        logger.error(f"❌ Failed to connect to Redis: {e}")
        return
    
    # Create workers
    post_worker = PostSyncWorker(redis_client)
    user_worker = UserSyncWorker(redis_client)
    
    # Start workers in separate threads
    logger.info("🔄 Starting CDC event consumers...")
    post_thread = threading.Thread(target=post_worker.start, daemon=True, name="PostWorker")
    user_thread = threading.Thread(target=user_worker.start, daemon=True, name="UserWorker")
    
    post_thread.start()
    user_thread.start()
    
    logger.info("✅ All workers started")
    logger.info("📡 Consuming CDC events from Redis Streams:")
    logger.info("   - dbz.trotot.Post (for posts)")
    logger.info("   - dbz.trotot.Customer (for users)")
    
    try:
        # Keep main thread alive
        while True:
            time.sleep(60)
            logger.debug("⏱️  Sync service heartbeat...")
    except KeyboardInterrupt:
        logger.info("👋 Shutting down sync-service...")


if __name__ == "__main__":
    main()
