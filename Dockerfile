FROM python:3.13-slim


# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir poetry

# Set working directory
WORKDIR /app

# Copy poetry files first
COPY pyproject.toml poetry.lock* ./


# Install dependencies
RUN poetry config virtualenvs.create false \
    && poetry install --no-root --no-interaction --no-ansi

# Copy application code
COPY . .

# Install poetry
RUN pip install poetry

# Expose port
EXPOSE 8000

# Command to run the application
CMD ["poetry", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]