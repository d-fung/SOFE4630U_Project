FROM python:3.9-slim

WORKDIR /app

# Copy both the Python file and the requirements.txt file into /app
COPY visualizer_API.py requirements.txt /app/

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Make port 8000 available to the world outside this container
EXPOSE 8000

# Correct the CMD instruction to match the Python file name and app instance
CMD ["uvicorn", "visualizer_API:app", "--host", "0.0.0.0", "--port", "8000"]
