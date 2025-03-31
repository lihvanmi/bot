# Используем официальный образ Python
FROM python:3.10-slim

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Создаем рабочую директорию
WORKDIR /app

# Установка FFmpeg
RUN apt-get update && \
    apt-get install -y ffmpeg && \
    rm -rf /var/lib/apt/lists/*

# Копируем зависимости
COPY requirements.txt .

# Устанавливаем зависимости Python
RUN pip install --upgrade pip && \
    pip install --no-cache-dir \
    --use-deprecated=legacy-resolver \
    -r requirements.txt

# Копируем исходный код
COPY . .


# Установка зависимостей
RUN pip install --no-cache-dir -r requirements.txt
