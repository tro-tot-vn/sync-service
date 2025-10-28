import torch
from typing import List, Dict
from loguru import logger
from config.settings import settings
from transformers import AutoModel

class EmbeddingService:
    def __init__(self):
        self.device = settings.DEVICE if torch.cuda.is_available() else "cpu"
        logger.info(f"Loading custom BGE-M3 model from Hugging Face on {self.device}...")
        
        # Load model using AutoModel (handles custom architecture automatically)
        # Model: https://huggingface.co/lamdx4/bge-m3-vietnamese-rental-projection
        self.model = AutoModel.from_pretrained(
            settings.EMBEDDING_MODEL,
            trust_remote_code=True  # Required for custom model architecture
        )
        
        self.model = self.model.to(self.device)
        self.model.eval()
        
        logger.info("✅ Custom BGE-M3 projection model loaded from Hugging Face")
        logger.info(f"   Model: {settings.EMBEDDING_MODEL}")
        logger.info(f"   Output dimension: {settings.EMBEDDING_DIM}")
        logger.info(f"   Device: {self.device}")
    
    def generate_dense_embedding(self, text: str, dim: int = None) -> List[float]:
        """
        Generate dense embedding using trained projection model
        
        Args:
            text: Input text
            dim: Output dimension (fixed at 128 for this model)
        
        Returns:
            List of floats (L2-normalized embedding vector, 128-dim)
        """
        # Use model's encode method (from HF model card)
        with torch.no_grad():
            embedding = self.model.encode([text], device=self.device)  # [1, 128]
            embedding = embedding[0].cpu().tolist()  # Convert to list
        
        return embedding
    
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
            Diện tích: {data['acreage']}m2
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


