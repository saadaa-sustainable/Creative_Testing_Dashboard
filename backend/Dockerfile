# Multi-stage build for the FastAPI backend.
# Produces a ~200 MB image ready to deploy to Render / Fly / Cloud Run / etc.
#
# Build locally:
#   docker build -t ctd-backend -f backend/Dockerfile .
# Run locally:
#   docker run -p 8000:8000 --env-file backend/.env ctd-backend
# Health check:
#   curl http://localhost:8000/api/health

FROM python:3.11-slim-bookworm AS base

# System deps for psycopg2 + healthy image size
RUN apt-get update && apt-get install -y --no-install-recommends \
      curl \
      libpq5 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first for better layer caching. Copy just the
# requirements file so unchanged deps don't invalidate the layer when
# app code changes.
COPY backend/requirements.web.txt /app/requirements.web.txt
RUN pip install --no-cache-dir -r /app/requirements.web.txt

# App code + static assets. The FastAPI serves index_v2.html + /assets/
# and (once you cut over) the React dist/, so both must land in the image.
COPY backend/           /app/backend/
COPY assets/            /app/assets/
COPY index_v2.html      /app/index_v2.html
# COPY dist/            /app/dist/         # uncomment when React port is ready

# Non-root user for prod. Render/Fly/etc. don't require this but it's
# a good hygiene default.
RUN useradd --create-home --shell /bin/bash app && chown -R app:app /app
USER app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=8000

EXPOSE 8000

# Healthcheck — Render/Fly use this to know the service is up.
# Curl-based (not python) so the check exits fast under load.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD curl -f http://localhost:${PORT:-8000}/api/health || exit 1

# Uvicorn with 1 worker (matches our in-process cache design). Bind to
# 0.0.0.0 so the container is reachable from the host / load balancer.
# Render/Fly override PORT via env — CMD respects it.
CMD ["sh", "-c", "cd /app/backend && python -m uvicorn api_ae:app --host 0.0.0.0 --port ${PORT:-8000} --workers 1"]
