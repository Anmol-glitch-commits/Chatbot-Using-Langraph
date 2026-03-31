# Use an official Python runtime as a parent image
FROM python:3.11-slim

#set the working directory
WORKDIR /app

#copy all the requirements
COPY requirements.txt .

#install all the requirements with pip
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the backend code
COPY . .

# Expose the port FastAPI will run on
EXPOSE 8000

# Command to run the application using uvicorn
# 0.0.0.0 allows the container to be accessed from outside
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
