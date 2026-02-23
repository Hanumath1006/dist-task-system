FROM python:3.9-slim

WORKDIR /app
COPY services/api/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY generated/ /app/generated/
COPY services/api/app.py /app/app.py

ENV PYTHONUNBUFFERED=1
EXPOSE 50050
CMD ["python", "app.py"]