# 📁 Dockerfile (клади у корінь Data_Docker)

FROM python:3.13-slim


# Оновлення pip, встановлення залежностей для python-binance (іноді потрібен gcc)
RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копіюємо requirements та встановлюємо залежності
COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# Копіюємо весь проект
COPY . .

# Cтворити директорію для даних всередині контейнера
RUN mkdir -p /app/data

# За замовчуванням запускати collector.py
CMD ["python", "-u", "collector.py"]

