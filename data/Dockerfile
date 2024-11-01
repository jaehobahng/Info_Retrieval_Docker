# Use Python as the base image
FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Copy the requirements and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy only the scripts (not the data files) into the image
COPY load_data.py .

# Run the data loading script
CMD ["python", "/app/load_data.py"]
