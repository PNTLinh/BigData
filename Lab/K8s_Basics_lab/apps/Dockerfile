FROM python:3.10-slim

WORKDIR /app

COPY simple-app.py .
COPY requirements-app.txt .

RUN pip install --no-cache-dir -r requirements-app.txt

EXPOSE 5000

CMD ["python", "simple-app.py"]

