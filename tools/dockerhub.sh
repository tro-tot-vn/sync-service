#!/bin/bash

# Build and push Docker image to DockerHub
# Reads version tag from metadata.json

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Read tag from metadata.json
TAG=$(cat "$SCRIPT_DIR/metadata.json" | grep -o '"tag"[[:space:]]*:[[:space:]]*"[^"]*"' | cut -d'"' -f4)

if [ -z "$TAG" ]; then
    echo "❌ Error: Could not read tag from metadata.json"
    exit 1
fi

IMAGE_NAME="lamdx4/sync-service-trototvn"

echo "🔧 Building $IMAGE_NAME:$TAG"
docker build -t "$IMAGE_NAME:$TAG" -t "$IMAGE_NAME:latest" "$PROJECT_DIR"

echo "📤 Pushing $IMAGE_NAME:$TAG"
docker push "$IMAGE_NAME:$TAG"

echo "📤 Pushing $IMAGE_NAME:latest"
docker push "$IMAGE_NAME:latest"

echo "✅ Done! Pushed $IMAGE_NAME:$TAG and $IMAGE_NAME:latest"
