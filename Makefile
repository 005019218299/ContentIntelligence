# Content Intelligence V9 - Docker Deployment Makefile

.PHONY: help build dev prod clean logs health test

# Default target
help:
	@echo "Content Intelligence V9 - Docker Commands"
	@echo "========================================"
	@echo "build     - Build Docker images"
	@echo "dev       - Start development environment"
	@echo "prod      - Start production environment"
	@echo "stop      - Stop all services"
	@echo "clean     - Clean up containers and images"
	@echo "logs      - Show application logs"
	@echo "health    - Check service health"
	@echo "test      - Run API tests"

# Build Docker images
build:
	@echo "🔨 Building Content Intelligence V9..."
	docker-compose build --no-cache

# Development environment
dev:
	@echo "🚀 Starting development environment..."
	docker-compose up -d
	@echo "✅ Development environment started!"
	@echo "🌐 API: http://localhost:8000"

# Production environment
prod:
	@echo "🚀 Starting production environment..."
	docker-compose -f docker-compose.prod.yml up -d
	@echo "✅ Production environment started!"
	@echo "🌐 API: http://localhost:8000"
	@echo "📊 Monitoring: http://localhost:3000"

# Stop services
stop:
	@echo "🛑 Stopping services..."
	docker-compose down
	docker-compose -f docker-compose.prod.yml down

# Clean up
clean:
	@echo "🧹 Cleaning up..."
	docker-compose down -v --rmi all
	docker system prune -f

# Show logs
logs:
	@echo "📋 Showing logs..."
	docker-compose logs -f content-intelligence-v9

# Health check
health:
	@echo "🔍 Checking health..."
	@curl -f http://localhost:8000/py/v9/health || echo "❌ Service not healthy"

# API tests
test:
	@echo "🧪 Running API tests..."
	@curl -X POST http://localhost:8000/py/v9/optimized-intelligence \
		-H "Content-Type: application/json" \
		-d '{"mode":"optimized_analysis","content_data":{"title":"Test","body":"Test content"}}' \
		|| echo "❌ API test failed"