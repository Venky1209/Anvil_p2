FROM python:3.13-slim

WORKDIR /app
COPY requirements.txt .
RUN apt-get update \
    && apt-get install -y --no-install-recommends bash \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir -r requirements.txt

COPY . /app/Anvil
ENV PYTHONPATH=/app
ENV ANVIL_BENCH_DIR=/bench

WORKDIR /app/Anvil
CMD ["bash", "bench/run.sh"]
