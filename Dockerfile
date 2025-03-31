# Установка FFmpeg
RUN apt-get update && \
    apt-get install -y ffmpeg && \
    rm -rf /var/lib/apt/lists/*

# Установка зависимостей
RUN pip install --no-cache-dir -r requirements.txt
