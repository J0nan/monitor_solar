FROM python:3.11-slim

WORKDIR /app
RUN apt-get update && apt-get install -y build-essential tzdata && rm -rf /var/lib/apt/lists/*

# Set container timezone and app tz (local day logic)
ENV TZ=Europe/Madrid
ENV APP_TZ=Europe/Madrid
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

RUN mkdir -p /app/data
COPY app /app/app

ENV PYTHONUNBUFFERED=1
EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--loop", "asyncio", "--workers", "1"]
