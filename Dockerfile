# Image for the ETL pipeline. The same image runs the Prefect server, the scheduled flow, and one-off commands.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PREFECT_HOME=/app/.prefect

# tzdata: the schedule is defined in America/New_York and needs the timezone database
RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

# run as a non-root user
RUN useradd --create-home --uid 1000 app
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY config.py extract.py validate.py transform.py load.py etl_pipeline.py ./
COPY scripts ./scripts

# PREFECT_HOME must be writable; a named volume mounted here inherits this ownership
RUN mkdir -p /app/.prefect && chown -R app:app /app
USER app

# default command = the daily Prefect schedule
CMD ["python", "etl_pipeline.py"]
