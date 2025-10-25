import threading
from loguru import logger
from services import get_milvus_service
from workers import PostSyncWorker, UserSyncWorker

def main():
    logger.info("🚀 Starting sync-service (Python)...")
    
    # Initialize Milvus
    try:
        milvus_service = get_milvus_service()
        milvus_service.initialize()
    except Exception as e:
        logger.error(f"Failed to initialize Milvus: {e}")
        return
    
    # Create workers
    post_worker = PostSyncWorker()
    user_worker = UserSyncWorker()
    
    # Start workers in separate threads
    post_thread = threading.Thread(target=post_worker.run, daemon=True)
    user_thread = threading.Thread(target=user_worker.run, daemon=True)
    
    post_thread.start()
    user_thread.start()
    
    logger.info("✅ All workers started")
    logger.info("📡 Listening for jobs from Redis queue...")
    
    try:
        # Keep main thread alive
        post_thread.join()
        user_thread.join()
    except KeyboardInterrupt:
        logger.info("👋 Shutting down sync-service...")

if __name__ == "__main__":
    main()


