FROM python:3.9-slim

WORKDIR /app
COPY services/worker/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY generated/ /app/generated/
COPY services/worker/app.py /app/app.py

ENV PYTHONUNBUFFERED=1
EXPOSE 50052
CMD ["python", "app.py"]