# Custom Airflow image with pre-installed dependencies
FROM apache/airflow:2.9.1-python3.11

# Install all required packages
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
