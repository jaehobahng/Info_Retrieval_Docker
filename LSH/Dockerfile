# src/Dockerfile

# Use a base Python image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy only the Poetry files first for dependency installation
COPY pyproject.toml poetry.lock /app/

# Install Poetry
RUN pip install poetry

# Install dependencies
RUN poetry config virtualenvs.create false && poetry install --no-dev

# Copy the rest of the application code
COPY . /app/

RUN pip install -e .

# Set the entry point to run your package (if needed)
CMD ["tail", "-f", "/dev/null"]
# CMD ["python", "src/deduplication/load.py"]
# CMD ["python", "src/deduplication/main.py"]
