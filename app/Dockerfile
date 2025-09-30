FROM python:3.11-slim

WORKDIR /opt/pipeline

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY pipeline_pg.py ./

CMD ["python", "pipeline_pg.py", "--help"]
