FROM python:3.11-slim

# System dependencies for Chromium (patchright uses a bundled Chromium)
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget gnupg ca-certificates fonts-liberation \
    libasound2 libatk-bridge2.0-0 libatk1.0-0 libcairo2 libcups2 \
    libdbus-1-3 libdrm2 libexpat1 libgbm1 libglib2.0-0 libgtk-3-0 \
    libnspr4 libnss3 libpango-1.0-0 libx11-6 libxcb1 libxcomposite1 \
    libxdamage1 libxext6 libxfixes3 libxkbcommon0 libxrandr2 libxshmfence1 \
    xdg-utils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first for better layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install bundled Chromium for patchright
RUN python -m patchright install chromium

# Copy app code
COPY . .

ENV PYTHONUNBUFFERED=1 \
    DATA_DIR=/data

# Default command — runs the UAE monitor. Override via Railway start command if needed.
CMD ["python", "monitor_uae.py"]
