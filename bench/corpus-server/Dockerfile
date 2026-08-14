FROM python:3.13-slim
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
COPY server.py /app/server.py
WORKDIR /app
ENTRYPOINT ["python3", "server.py"]
