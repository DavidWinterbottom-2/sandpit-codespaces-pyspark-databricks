# Alternative Dockerfile for local development
FROM mcr.microsoft.com/devcontainers/python:1-3.11

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    unzip \
    lsof \
    openjdk-17-jre-headless \
    openjdk-17-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Install Miniconda
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh \
    && bash /tmp/miniconda.sh -b -p /opt/conda \
    && rm /tmp/miniconda.sh \
    && /opt/conda/bin/conda clean -ya

# Add conda to PATH
ENV PATH="/opt/conda/bin:$PATH"

# Initialize conda
RUN /opt/conda/bin/conda init bash

# Set working directory
WORKDIR /workspace

# Copy environment files
COPY environments/ ./environments/

# Create conda environments
## Accept Anaconda Terms of Service for non-interactive builds (required for repo.anaconda channels)
RUN /opt/conda/bin/conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main && \
    /opt/conda/bin/conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r || true

# Create base conda environments (delay optional/large envs to post-create to keep image builds stable)
RUN /opt/conda/bin/conda env create -f environments/base-pyspark.yml && \
    /opt/conda/bin/conda env create -f environments/streaming.yml && \
    /opt/conda/bin/conda clean -ya

# Note: delta-lake.yml and unity-catalog.yml are intentionally NOT created here because
# they include pip packages that are large or platform-specific and are better handled at
# container runtime by the post-create script.

# Copy the rest of the project
COPY . .

# Create data directories
RUN mkdir -p data/{input,output,checkpoints,delta,warehouse,metadata,lineage} logs

# Set permissions
RUN chmod +x scripts/*.sh

# Expose ports
EXPOSE 4040 8080 8888 9092

# Default command
CMD ["/bin/bash"]