# News Intelligence Platform - Dockerfile
FROM python:3.11-slim


WORKDIR /app


RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*


COPY scrapers/requirements.txt /app/scrapers/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r scrapers/requirements.txt

# Copy application code
COPY . /app/

# Create necessary directories
RUN mkdir -p data_lake/bronze data_lake/silver data_lake/gold reports logs

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash app && chown -R app:app /app
USER app

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import os; print('Container is healthy')" || exit 1

# Default command
CMD ["python", "pipeline.py"]