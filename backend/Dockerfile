FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml /app/
COPY app /app/app
COPY scripts/wait-for-deps.sh /app/scripts/wait-for-deps.sh
RUN apt-get update && apt-get install -y --no-install-recommends netcat && rm -rf /var/lib/apt/lists/*
RUN chmod +x /app/scripts/wait-for-deps.sh

RUN pip install --no-cache-dir -e .[redis,kafka]

EXPOSE 8000

ENTRYPOINT ["/app/scripts/wait-for-deps.sh"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
