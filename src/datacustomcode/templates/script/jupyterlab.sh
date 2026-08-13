#!/bin/bash

# Description: This script is used to start/stop the jupyter notebook in a docker container

# Function to open browser based on OS
open_browser() {
    local url=$1
    case "$(uname -s)" in
        Darwin*)  # macOS
            open "$url"
            ;;
        Linux*)   # Linux
            if command -v xdg-open &> /dev/null; then
                xdg-open "$url"
            elif command -v gnome-open &> /dev/null; then
                gnome-open "$url"
            else
                echo "Could not detect the web browser to use"
                return 1
            fi
            ;;
        CYGWIN*|MINGW32*|MSYS*|MINGW*)  # Windows
            start "$url"
            ;;
        *)
            echo "Unknown operating system"
            return 1
            ;;
    esac
}

# Function to check if Docker is installed and running
check_docker() {
    if ! command -v docker &> /dev/null; then
        echo "Docker is not installed"
        exit 1
    fi
    echo "Docker is installed"
    docker --version

    if ! docker info &> /dev/null; then
        echo "Docker daemon is not running"
        exit 1
    fi
    echo "Docker daemon is running"
}

# Function to check if openssl is installed
check_openssl() {
    if ! command -v openssl &> /dev/null; then
        echo "openssl is not installed. It is required to generate a secure JupyterLab access token."
        exit 1
    fi
}

# Function to start Jupyter server
start_jupyter() {
    echo "Building the docker image"
    docker build -t datacloud-customcode .

    local TOKEN
    TOKEN=$(openssl rand -hex 32)

    echo "Running the docker container"
    docker run -d --rm -p 127.0.0.1:8888:8888 \
        -v $(pwd):/workspace \
        --name jupyter-server \
        datacloud-customcode jupyter lab \
        --ip=0.0.0.0 \
        --port=8888 \
        --no-browser \
        --allow-root \
        --NotebookApp.token="$TOKEN" \
        --notebook-dir=/workspace

    sleep 3  # Wait for server to start
    local URL
    URL="http://localhost:8888/?token=$TOKEN"
    echo "Opening $URL"
    open_browser $URL
}

# Function to stop Jupyter server
stop_jupyter() {
    echo "Stopping Jupyter server container..."
    if docker ps -q --filter "name=jupyter-server" | grep -q .; then
        docker stop jupyter-server
        echo "Jupyter server stopped successfully"
    else
        echo "No Jupyter server container running"
    fi
}

# Main script logic
case "$1" in
    "start")
        check_docker
        check_openssl
        start_jupyter
        ;;
    "stop")
        check_docker
        stop_jupyter
        ;;
    *)
        echo "Usage: $0 {start|stop}"
        echo "  start - Start Jupyter server"
        echo "  stop  - Stop Jupyter server"
        exit 1
        ;;
esac
