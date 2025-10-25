FROM spark:4.0.0-scala2.13-java21-python3-ubuntu

USER root

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .

EXPOSE 5000
CMD ["python3", "app.py"]
