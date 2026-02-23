FROM python:3.9-slim

WORKDIR /app
COPY services/scheduler/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY generated/ /app/generated/
COPY services/scheduler/app.py /app/app.py

ENV PYTHONUNBUFFERED=1
EXPOSE 50051
CMD ["python", "app.py"]