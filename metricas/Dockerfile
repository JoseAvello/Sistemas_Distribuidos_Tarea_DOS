FROM python:3.11-slim

WORKDIR /app

RUN pip install flask redis requests kafka-python

COPY app.py .

CMD ["python", "app.py"]
