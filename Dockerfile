FROM python:3.9.1

RUN pip install pandas 
RUN pip install sqlalchemy 
RUN pip install psycopg2

WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]