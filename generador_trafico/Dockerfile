FROM python:3.11-slim

WORKDIR /app

RUN pip install requests kafka-python

COPY app.py .

CMD ["python", "app.py"]
