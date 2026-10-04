FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY libs/aml_common /tmp/aml_common
RUN pip install /tmp/aml_common
COPY services/graph_analytics/requirements.txt .
RUN pip install -r requirements.txt
COPY services/graph_analytics/graph_analytics ./graph_analytics
CMD ["python", "-m", "graph_analytics.main"]
