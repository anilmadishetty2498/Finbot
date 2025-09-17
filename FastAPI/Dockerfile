# Use official Python image as base
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the FastAPI application files
COPY . .

# Add PYTHONPATH so src/ can be imported from app/
ENV PYTHONPATH=/app
 
RUN ls -ltr

# Expose the port the FastAPI app runs on
EXPOSE 8000

# Command to run the FastAPI application
#CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

#CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload", "--reload-include", "Data/dummy_data.csv"]

#CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "app.main:app", "--workers=4", "--threads=8", "--bind=0.0.0.0:8000"]
