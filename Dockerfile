FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Pre-download custom projection model from HuggingFace (cache in Docker layer)
RUN python -c "from transformers import AutoModel; \
    AutoModel.from_pretrained('lamdx4/bge-m3-vietnamese-rental-projection', trust_remote_code=True)" || true

# Run application
CMD ["python", "app.py"]


