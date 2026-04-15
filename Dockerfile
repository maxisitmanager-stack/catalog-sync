FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY catalog_sync.py .

ENV PORT=8080

CMD ["python", "catalog_sync.py"]
