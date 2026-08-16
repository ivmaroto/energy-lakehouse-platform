FROM apache/superset:6.1.0

USER root

COPY docker/superset/requirements.txt /app/requirements.txt

RUN . /app/.venv/bin/activate && \
    uv pip install --no-cache \
    -r /app/requirements.txt

COPY docker/superset/superset_config.py /app/superset_config.py

ENV SUPERSET_CONFIG_PATH=/app/superset_config.py

USER superset