from pymilvus import (
    connections,
    Collection,
    CollectionSchema,
    FieldSchema,
    DataType,
    Function,
    FunctionType,
    utility
)
from loguru import logger
from config.settings import settings

class MilvusService:
    def __init__(self):
        self.host = settings.MILVUS_HOST
        self.port = settings.MILVUS_PORT
        self.posts_collection = "posts_hybrid"
        
        connections.connect(
            alias="default",
            host=self.host,
            port=str(self.port)
        )
        logger.info(f"✅ Connected to Milvus at {self.host}:{self.port}")
    
    def initialize(self):
        """Initialize collections"""
        self._create_posts_collection()
        logger.info("✅ Milvus collections initialized")
    
    def _create_posts_collection(self):
        """Create posts collection with multi-field BM25"""
        if utility.has_collection(self.posts_collection):
            logger.info(f"Collection {self.posts_collection} already exists")
            return
        
        logger.info(f"Creating collection {self.posts_collection}...")
        
        # Define fields
        fields = [
            # Primary key
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=False),
            
            # Dense vector (BGE-M3 semantic)
            FieldSchema(name="dense_vector", dtype=DataType.FLOAT_VECTOR, dim=128),
            
            # Text inputs for BM25
            FieldSchema(
                name="title",
                dtype=DataType.VARCHAR,
                max_length=500,
                enable_analyzer=True,
                analyzer_params={"type": "standard"}
            ),
            FieldSchema(
                name="description",
                dtype=DataType.VARCHAR,
                max_length=5000,
                enable_analyzer=True,
                analyzer_params={"type": "standard"}
            ),
            FieldSchema(
                name="address_text",
                dtype=DataType.VARCHAR,
                max_length=500,
                enable_analyzer=True,
                analyzer_params={"type": "standard"}
            ),
            
            # Sparse vectors (outputs - auto-generated)
            FieldSchema(name="sparse_title", dtype=DataType.SPARSE_FLOAT_VECTOR),
            FieldSchema(name="sparse_description", dtype=DataType.SPARSE_FLOAT_VECTOR),
            FieldSchema(name="sparse_address", dtype=DataType.SPARSE_FLOAT_VECTOR),
            
            # Scalar fields for filtering
            FieldSchema(name="price", dtype=DataType.INT64),
            FieldSchema(name="acreage", dtype=DataType.INT32),
            FieldSchema(name="city", dtype=DataType.VARCHAR, max_length=100),
            FieldSchema(name="district", dtype=DataType.VARCHAR, max_length=100),
            FieldSchema(name="ward", dtype=DataType.VARCHAR, max_length=100),
            FieldSchema(name="street", dtype=DataType.VARCHAR, max_length=200),
            FieldSchema(name="interior_condition", dtype=DataType.VARCHAR, max_length=20),
            
            # Metadata
            FieldSchema(name="owner_id", dtype=DataType.INT64),
            FieldSchema(name="created_at", dtype=DataType.INT64),
            FieldSchema(name="extended_at", dtype=DataType.INT64),
        ]
        
        schema = CollectionSchema(
            fields=fields,
            description="Posts with multi-field BM25",
            enable_dynamic_field=True
        )
        
        # Add BM25 Functions
        bm25_title = Function(
            name="bm25_title",
            input_field_names=["title"],
            output_field_names=["sparse_title"],
            function_type=FunctionType.BM25
        )
        schema.add_function(bm25_title)
        
        bm25_desc = Function(
            name="bm25_description",
            input_field_names=["description"],
            output_field_names=["sparse_description"],
            function_type=FunctionType.BM25
        )
        schema.add_function(bm25_desc)
        
        bm25_addr = Function(
            name="bm25_address",
            input_field_names=["address_text"],
            output_field_names=["sparse_address"],
            function_type=FunctionType.BM25
        )
        schema.add_function(bm25_addr)
        
        # Create collection
        collection = Collection(name=self.posts_collection, schema=schema)
        
        # Create indexes
        collection.create_index(
            field_name="dense_vector",
            index_params={
                "index_type": "HNSW",
                "metric_type": "COSINE",
                "params": {"M": 16, "efConstruction": 256}
            }
        )
        
        for sparse_field in ["sparse_title", "sparse_description", "sparse_address"]:
            collection.create_index(
                field_name=sparse_field,
                index_params={
                    "index_type": "SPARSE_INVERTED_INDEX",
                    "metric_type": "BM25"
                }
            )
        
        collection.create_index(
            field_name="price",
            index_params={"index_type": "STL_SORT"}
        )
        collection.create_index(
            field_name="city",
            index_params={"index_type": "TRIE"}
        )
        
        collection.load()
        logger.info(f"✅ Collection {self.posts_collection} created with multi-field BM25")
    
    def upsert_post(self, post_id: int, dense_vec: list, data: dict):
        """Upsert post with multi-field BM25"""
        collection = Collection(self.posts_collection)
        
        # Delete existing
        expr = f"id == {post_id}"
        collection.delete(expr)
        
        # Prepare address text
        address_text = f"{data['street']}, {data['ward']}, {data['district']}, {data['city']}"
        
        # Convert dates to timestamps
        created_at = int(data['createdAt'].timestamp()) if hasattr(data['createdAt'], 'timestamp') else int(data['createdAt'])
        extended_at = int(data['extendedAt'].timestamp()) if hasattr(data['extendedAt'], 'timestamp') else int(data['extendedAt'])
        
        # Insert
        entities = [{
            "id": post_id,
            "dense_vector": dense_vec,
            "title": data["title"],
            "description": data["description"],
            "address_text": address_text,
            "price": data["price"],
            "acreage": data["acreage"],
            "city": data["city"],
            "district": data["district"],
            "ward": data["ward"],
            "street": data["street"],
            "interior_condition": data["interiorCondition"],
            "owner_id": data["ownerId"],
            "created_at": created_at,
            "extended_at": extended_at,
        }]
        
        collection.insert(entities)
        collection.flush()
        logger.info(f"✅ Upserted post {post_id} (3 sparse vectors auto-generated)")
    
    def delete_post(self, post_id: int):
        """Delete post from Milvus"""
        collection = Collection(self.posts_collection)
        expr = f"id == {post_id}"
        collection.delete(expr)
        logger.info(f"✅ Deleted post {post_id}")

# Singleton
_milvus_service = None

def get_milvus_service():
    global _milvus_service
    if _milvus_service is None:
        _milvus_service = MilvusService()
    return _milvus_service


