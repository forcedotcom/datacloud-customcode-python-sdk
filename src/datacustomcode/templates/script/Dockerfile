FROM public.ecr.aws/emr-on-eks/spark/emr-7.3.0:latest

USER root

# install from dev requirements.txt
COPY requirements-dev.txt ./requirements-dev.txt
RUN pip3.11 install --no-cache-dir -r requirements-dev.txt

# Install from requirements.txt:
COPY requirements.txt ./requirements.txt
RUN pip3.11 install --no-cache-dir -r requirements.txt

# Create workspace directory
RUN mkdir /workspace

# Set user and working directory
USER hadoop:hadoop
WORKDIR /workspace
