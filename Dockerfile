# Dockerfile
# Use an official Python runtime as a parent image
FROM python:3.9-slim-buster

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Expose the port that the application will listen on
EXPOSE 8080

# Run the Gunicorn web server with your Flask app
# 'main:app' means the 'app' object in 'main.py'
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
