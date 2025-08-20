#!/bin/bash

# Content Intelligence V9 Deployment Script
set -e

echo "🚀 Starting Content Intelligence V9 Deployment..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p logs models cache ssl

# Build and deploy
echo "🔨 Building Docker images..."
docker-compose build --no-cache

echo "🚀 Starting services..."
docker-compose up -d

# Wait for services to be ready
echo "⏳ Waiting for services to start..."
sleep 30

# Health check
echo "🔍 Performing health check..."
if curl -f http://localhost:8000/py/v9/health > /dev/null 2>&1; then
    echo "✅ Content Intelligence V9 is running successfully!"
    echo "🌐 API available at: http://localhost:8000"
    echo "📊 Health check: http://localhost:8000/py/v9/health"
    echo "🌱 Sustainability dashboard: http://localhost:8000/py/v9/sustainability-dashboard"
else
    echo "❌ Health check failed. Checking logs..."
    docker-compose logs content-intelligence-v9
    exit 1
fi

echo "🎉 Deployment completed successfully!"