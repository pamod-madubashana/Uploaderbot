FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY . .

RUN useradd --create-home --shell /bin/bash botuser \
    && mkdir -p /app/downloads \
    && chown -R botuser:botuser /app

USER botuser

CMD ["python", "__main__.py"]
