FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg gcc libc6-dev \
    && pip install --no-cache-dir ffsubsync==0.4.25 \
    && apt-get purge -y gcc libc6-dev && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY sub_fetcher.py .

CMD ["python3", "-u", "sub_fetcher.py"]
