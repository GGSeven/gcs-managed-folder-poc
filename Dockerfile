FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir requests
COPY reconcile.py .
CMD ["python", "reconcile.py"]
