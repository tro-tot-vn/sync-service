import torch
from FlagEmbedding import BGEM3FlagModel
from typing import List, Dict
from loguru import logger
from config.settings import settings

class EmbeddingService:
    def __init__(self):
        self.device = settings.DEVICE if torch.cuda.is_available() else "cpu"
        logger.info(f"Loading BGE-M3 model on {self.device}...")
        
        self.model = BGEM3FlagModel(
            settings.EMBEDDING_MODEL,
            use_fp16=True if self.device == "cuda" else False
        )
        
        logger.info("✅ BGE-M3 model loaded successfully")
    
    def generate_dense_embedding(self, text: str, dim: int = None) -> List[float]:
        """
        Generate dense embedding with custom dimension
        
        Args:
            text: Input text
            dim: Output dimension (default from settings)
        
        Returns:
            List of floats (embedding vector)
        """
        if dim is None:
            dim = settings.EMBEDDING_DIM
        
        # Generate full embedding (1024 dims for BGE-M3)
        embeddings = self.model.encode(
            [text],
            batch_size=1,
            max_length=512
        )['dense_vecs']
        
        # Reduce dimension (simple truncation)
        reduced_embedding = embeddings[0][:dim].tolist()
        
        return reduced_embedding
    
    def prepare_post_text(self, data: Dict) -> str:
        """
        Prepare post data for embedding (dense vector only)
        This combines all fields for semantic understanding
        """
        return f"""
{data['title']}
{data['description']}
Địa chỉ: {data['street']}, {data['ward']}, {data['district']}, {data['city']}
Giá: {data['price']:,} VNĐ/tháng
Diện tích: {data['acreage']}m²
Nội thất: {data['interiorCondition']}
        """.strip()
    
    def prepare_user_text(self, data: Dict) -> str:
        """Prepare user data for embedding"""
        parts = [
            f"{data['firstName']} {data['lastName']}",
        ]
        
        if data.get('bio'):
            parts.append(data['bio'])
        
        if data.get('currentJob'):
            parts.append(f"Nghề nghiệp: {data['currentJob']}")
        
        if data.get('address'):
            parts.append(f"Địa chỉ: {data['address']}")
        
        return "\n".join(parts)

# Singleton instance
_embedding_service = None

def get_embedding_service():
    global _embedding_service
    if _embedding_service is None:
        _embedding_service = EmbeddingService()
    return _embedding_service


