# Pull base image
FROM python:3.10.2-slim-bullseye@sha256:6faf002f0bce2ce81bec4a2253edddf0326dad23fe4e95e90d7790eaee653da5

# Set environment variables
ENV PIP_DISABLE_PIP_VERSION_CHECK 1
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set work directory
WORKDIR /code

# Install dependencies
COPY ./requirements.txt .
RUN pip install -r requirements.txt

# Copy project
COPY . .
