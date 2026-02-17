FROM python:3.13.11-slim

RUN pip install --upgrade pip pandas pyarrow sqlalchemy psycopg2-binary tqdm

WORKDIR /code

COPY ingest_data.py .

ENTRYPOINT ["python3", "ingest_data.py"]