# ============================================
# Stage 1: Builder - Install dependencies (CPU-only)
# ============================================
FROM python:3.11-slim as builder

WORKDIR /app

# Install system dependencies for building Python packages
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install PyTorch CPU-only FIRST (from official CPU wheel)
# This avoids pulling CUDA dependencies (~10GB savings)
RUN pip install --user --no-cache-dir \
    torch --index-url https://download.pytorch.org/whl/cpu

# Install remaining dependencies
RUN pip install --user --no-cache-dir -r requirements.txt

# ============================================
# Stage 2: Runtime - Optimized final image
# ============================================
FROM python:3.11-slim

# Create non-root user
RUN useradd -m -u 1000 appuser

WORKDIR /app

# Install only runtime dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy Python packages from builder
COPY --from=builder --chown=appuser:appuser /root/.local /home/appuser/.local

# Add user site-packages to PATH
ENV PATH=/home/appuser/.local/bin:$PATH

# Copy source code
COPY --chown=appuser:appuser . .

# Switch to non-root user
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import sys; from config.settings import settings; import redis; r=redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT); r.ping(); sys.exit(0)" || exit 1

# Run application
CMD ["python", "app.py"]
