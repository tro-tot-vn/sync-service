from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # Redis
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 5555
    
    # Milvus
    MILVUS_HOST: str = "localhost"
    MILVUS_PORT: int = 19531  # Remapped from 19530 for shared server
    
    # Model (Hugging Face)
    EMBEDDING_MODEL: str = "lamdx4/bge-m3-vietnamese-rental-projection"
    EMBEDDING_DIM: int = 128  # Fixed for this model
    DEVICE: str = "cuda"
    
    # Worker
    WORKER_CONCURRENCY: int = 5
    
    class Config:
        env_file = ".env"

settings = Settings()


