FROM python:3.10-slim
WORKDIR /app
COPY services/workload_generator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/workload_generator/ .
COPY common/ ./common/
ENV PYTHONUNBUFFERED=1
CMD ["python", "generator.py"]
