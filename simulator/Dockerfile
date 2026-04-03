FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN pip install -e /app/shared

ENV SIMULATOR_HOST=0.0.0.0
ENV SIMULATOR_PORT=8888
ENV SIMULATOR_DEVICE_COUNT=100

EXPOSE 8888

CMD ["python", "-m", "simulator.main"]
