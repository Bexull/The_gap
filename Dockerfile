# Используем официальный Python образ
FROM python:3.11-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Копируем файл зависимостей
COPY requirements.txt .

# Устанавливаем зависимости по одной для лучшего кэширования
RUN pip install --upgrade pip
RUN pip install --no-cache-dir python-telegram-bot
RUN pip install --no-cache-dir pandas
RUN pip install --no-cache-dir SQLAlchemy
RUN pip install --no-cache-dir python-dotenv

# Копируем весь проект
COPY . .

# Создаем директории для volume
RUN mkdir -p /app/data /app/logs

# Создаем volume для данных
VOLUME ["/app/data", "/app/logs"]

# Команда запуска
CMD ["python", "main.py"]
