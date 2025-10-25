"""
Bulk load existing data from MS SQL to Milvus
Run this script after initial setup to index all existing posts and users
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pymssql
import redis
import json
from loguru import logger
from config.settings import settings

def connect_to_mssql():
    """Connect to MS SQL Server"""
    # You need to set these environment variables
    conn = pymssql.connect(
        server=os.getenv('DB_HOST', 'localhost'),
        user=os.getenv('DB_USERNAME', 'sa'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_DATABASE', 'TroTotVN')
    )
    return conn

def bulk_load_posts():
    """Load all approved posts"""
    logger.info("📦 Starting bulk load for posts...")
    
    conn = connect_to_mssql()
    cursor = conn.cursor(as_dict=True)
    
    # Query all approved posts
    cursor.execute("""
        SELECT 
            postId, title, description, price, acreage,
            city, district, ward, street, streetNumber,
            interiorCondition, ownerId, createdAt, extendedAt
        FROM Post 
        WHERE status = 'Approved'
    """)
    
    posts = cursor.fetchall()
    logger.info(f"Found {len(posts)} approved posts to index")
    
    # Connect to Redis
    redis_client = redis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        decode_responses=True
    )
    
    # Push to queue
    for post in posts:
        job_data = {
            "operation": "insert",
            "postId": post['postId'],
            "data": {
                "title": post['title'],
                "description": post['description'],
                "price": post['price'],
                "acreage": post['acreage'],
                "city": post['city'],
                "district": post['district'],
                "ward": post['ward'],
                "street": post['street'],
                "streetNumber": post['streetNumber'],
                "interiorCondition": post['interiorCondition'],
                "ownerId": post['ownerId'],
                "createdAt": post['createdAt'].timestamp() if hasattr(post['createdAt'], 'timestamp') else 0,
                "extendedAt": post['extendedAt'].timestamp() if hasattr(post['extendedAt'], 'timestamp') else 0
            }
        }
        
        redis_client.rpush('post-sync-simple', json.dumps(job_data))
    
    logger.info(f"✅ Queued {len(posts)} posts for indexing")
    
    conn.close()

def bulk_load_users():
    """Load all users"""
    logger.info("📦 Starting bulk load for users...")
    
    conn = connect_to_mssql()
    cursor = conn.cursor(as_dict=True)
    
    # Query all customers
    cursor.execute("""
        SELECT 
            customerId, firstName, lastName, gender,
            birthday, address, bio
        FROM Customer
    """)
    
    users = cursor.fetchall()
    logger.info(f"Found {len(users)} users to index")
    
    # Connect to Redis
    redis_client = redis.Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        decode_responses=True
    )
    
    # Push to queue
    for user in users:
        job_data = {
            "operation": "insert",
            "customerId": user['customerId'],
            "data": {
                "firstName": user['firstName'],
                "lastName": user['lastName'],
                "gender": user['gender'],
                "birthday": user.get('birthday'),
                "address": user.get('address'),
                "bio": user.get('bio')
            }
        }
        
        redis_client.rpush('user-sync-simple', json.dumps(job_data))
    
    logger.info(f"✅ Queued {len(users)} users for indexing")
    
    conn.close()

def main():
    """Main function"""
    logger.info("🚀 Starting bulk load...")
    
    try:
        bulk_load_posts()
        bulk_load_users()
        logger.info("✅ Bulk load completed successfully")
        logger.info("📊 Check sync-service logs to monitor progress")
    except Exception as e:
        logger.error(f"❌ Bulk load failed: {e}")
        raise

if __name__ == "__main__":
    main()


