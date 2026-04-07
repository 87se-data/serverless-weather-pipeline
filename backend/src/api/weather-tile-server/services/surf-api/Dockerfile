# Lightweight Python image
FROM python:3.11-slim

# Working directory
WORKDIR /app

# ここに追加！OS側にWebPのライブラリをインストール！
RUN apt-get update && apt-get install -y \
    libwebp-dev \
    && rm -rf /var/lib/apt/lists/*

# Dependencies
COPY services/surf-api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application and shared code
COPY services/surf-api /app/services/surf-api
COPY shared /app/shared

# Set PYTHONPATH to include the project root
ENV PYTHONPATH=/app

# Run the application
CMD ["uvicorn", "services.surf-api.main:app", "--host", "0.0.0.0", "--port", "8080"]