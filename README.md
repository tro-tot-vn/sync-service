# Sync Service

Python service for syncing data from MS SQL to Milvus vector database.

## Features

- **BGE-M3 Embeddings**: 128-dim custom embeddings
- **Multi-field BM25**: Separate BM25 for title, description, address
- **Redis Queue**: Consume jobs from BullMQ
- **Auto-sync**: Real-time sync on database changes

## Architecture

```
tro-tot-vn-be → Redis Queue → sync-service → Milvus
```

## Setup

1. Copy `.env.example` to `.env`:
```bash
cp .env.example .env
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Run:
```bash
python app.py
```

## Docker

```bash
docker build -t sync-service .
docker run -e REDIS_HOST=redis -e MILVUS_HOST=milvus sync-service
```

## Collections

### posts_hybrid
- Dense vector: BGE-M3 (128-dim)
- Sparse vectors: title, description, address (BM25 auto-generated)
- Scalars: price, acreage, city, district, ward

### users
- User embedding: 128-dim
- Metadata: name, gender, city


