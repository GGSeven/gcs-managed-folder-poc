FROM python:3.11-slim
RUN pip install --no-cache-dir requests
WORKDIR /app
COPY config.env reconcile_fast.py ./
RUN chmod +x reconcile_fast.py
ENTRYPOINT ["python3", "-u", "/app/reconcile_fast.py"]
