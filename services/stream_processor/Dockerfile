FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY libs/aml_common /tmp/aml_common
RUN pip install /tmp/aml_common
COPY services/stream_processor/requirements.txt .
RUN pip install -r requirements.txt
COPY services/stream_processor/stream_processor ./stream_processor
CMD ["python", "-m", "stream_processor.main"]
