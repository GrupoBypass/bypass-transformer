FROM bitnami/spark:3.5.0

USER root

WORKDIR /app
ENV PYTHONPATH=/app
RUN mkdir -p /home/spark/.ivy2/local && chown -R 1001:1001 /home/spark
ENV HOME=/home/spark
ENV _JAVA_OPTIONS="-Duser.home=/home/spark"
ENV SPARK_CONF_DIR=/opt/bitnami/spark/conf

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

USER 1001
WORKDIR /app

CMD ["python", "main.py"]