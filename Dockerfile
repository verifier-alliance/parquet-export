FROM python:3.12.2-slim

# Set the working directory
WORKDIR /app

# Install dependencies

RUN apt update && apt install -y libpq-dev gcc

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application. Keep the requirements in cache if source code changes
COPY . .

# Command to run the application
CMD ["python", "main.py"]
