FROM bitnamilegacy/spark:3.5
WORKDIR /app
COPY requirements.txt .
USER root
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

CMD ["spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3", \
     "streaming_job.py"]
