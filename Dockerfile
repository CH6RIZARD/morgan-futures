FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY bot/ /app/bot/
COPY dashboard/ /app/dashboard/
COPY gunicorn.conf.py /app/gunicorn.conf.py

EXPOSE 8080
ENV PORT=8080

CMD gunicorn bot.server:app -c gunicorn.conf.py --bind 0.0.0.0:$PORT --workers 1 --threads 4 --timeout 120
