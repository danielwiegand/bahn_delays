FROM python:3.9.4-slim

# Set the working directory inside the container to /app
WORKDIR /app

COPY requirements.txt /app

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

CMD ["python", "changes-kafka.py"]
