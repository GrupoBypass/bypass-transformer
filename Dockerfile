FROM spark:python3 as spark

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

ENTRYPOINT ["python"]
CMD ["main.py"]
