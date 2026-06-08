FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends magic-wormhole openssh-server \
    && mkdir -p /root/.ssh /var/run/sshd \
    && chmod 700 /root/.ssh \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY start.py .
COPY warm_school_cache.py .
COPY static ./static

EXPOSE 8000

CMD ["python", "start.py"]
