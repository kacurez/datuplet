FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY components/pandas-transform/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the Python SDK
COPY sdk/python /app/sdk/python

# Copy the component
COPY components/pandas-transform/main.py /app/main.py

# Run as non-root user
RUN useradd -m -u 1000 datuplet
USER datuplet

ENTRYPOINT ["python", "/app/main.py"]
