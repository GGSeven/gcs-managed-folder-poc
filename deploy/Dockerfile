FROM python:3.11-slim
WORKDIR /app
COPY config.env reconcile_fast.py ./
RUN chmod +x reconcile_fast.py
ENTRYPOINT ["python3", "/app/reconcile_fast.py"]
