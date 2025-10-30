# ============================================
# Stage 1: Builder - Install dependencies
# ============================================
FROM python:3.11-slim as builder

WORKDIR /app

# Install system dependencies for building Python packages
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install to user site-packages
COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

# ============================================
# Stage 2: Runtime - Optimized final image
# ============================================
FROM python:3.11-slim

# Create non-root user FIRST (before any files)
RUN useradd -m -u 1000 appuser

WORKDIR /app

# Install only runtime dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy Python packages from builder and set ownership immediately
COPY --from=builder --chown=appuser:appuser /root/.local /home/appuser/.local

# Add user site-packages to PATH
ENV PATH=/home/appuser/.local/bin:$PATH

# Copy source code with correct ownership
COPY --chown=appuser:appuser . .

# Switch to non-root user BEFORE downloading model (to avoid chown later)
USER appuser

# Pre-download custom projection model from HuggingFace (cache in Docker layer)
# This ensures the model is available offline in production
# Downloaded as appuser -> no chown needed -> no layer duplication
RUN python -c "from transformers import AutoModel; \
    AutoModel.from_pretrained('lamdx4/bge-m3-vietnamese-rental-projection', trust_remote_code=True)" || true

# Health check - verify Redis connection
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import sys; from config.settings import settings; import redis; r=redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT); r.ping(); sys.exit(0)" || exit 1

# Run application
CMD ["python", "app.py"]


