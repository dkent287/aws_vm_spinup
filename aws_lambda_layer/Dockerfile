# Use a Python image with package management tools
FROM python:3.12-slim AS builder

# Install zip
RUN apt-get update && apt-get install -y zip

# Create the target directory structure
RUN mkdir -p /opt/python/lib/python3.12/site-packages

# Install Paramiko and its dependencies
RUN pip install --target /opt/python/lib/python3.12/site-packages paramiko

# Install Pandas and its dependencies
RUN pip install --target /opt/python/lib/python3.12/site-packages pandas

# Create a zip file containing the installed libraries
RUN cd /opt && zip -r layer.zip python

# Use the AWS Lambda Python base image
FROM public.ecr.aws/lambda/python:3.12-x86_64

# Copy the zip file from the builder stage
COPY --from=builder /opt/layer.zip /opt/layer.zip
