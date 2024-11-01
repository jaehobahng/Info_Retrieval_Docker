# Use an official Python runtime as a base image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the current directory contents into the container at /app
COPY . .

# Expose the Flask app port
EXPOSE 5000

# Set environment variables for Flask
ENV FLASK_APP=app.py
ENV FLASK_ENV=development
ENV FLASK_RUN_RELOAD=true
ENV FLASK_DEBUG=1

# Command to run the Flask app
CMD ["flask", "run", "--host=0.0.0.0", "--port=5000"]
