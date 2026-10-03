FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY libs/aml_common /tmp/aml_common
RUN pip install /tmp/aml_common
COPY services/simulator/requirements.txt .
RUN pip install -r requirements.txt
COPY services/simulator/simulator ./simulator
CMD ["python", "-m", "simulator.main"]
