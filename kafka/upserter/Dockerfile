# Dockerfile
FROM python:3.11-slim

# (선택) 런타임에 유용한 툴 & CA
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY upserter.py /app/upserter.py

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8 PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
ENV PYTHONUNBUFFERED=1
CMD ["python", "/app/upserter.py"]
