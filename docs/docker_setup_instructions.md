# Docker Setup Instructions for YouTube ETL Project

This document provides instructions for setting up a MySQL database using Docker for the YouTube ETL project.

## Prerequisites

1. **Docker**: You need to have Docker installed on your system.
   - For macOS: Download and install [Docker Desktop for Mac](https://docs.docker.com/desktop/install/mac-install/)
   - For Windows: Download and install [Docker Desktop for Windows](https://docs.docker.com/desktop/install/windows-install/)
   - For Linux: Follow the [Docker Engine installation instructions](https://docs.docker.com/engine/install/)

2. **Python Dependencies**: Make sure you have the required Python packages installed:
   ```bash
   pip install -r requirements.txt
   ```

## Using the Docker Setup Script

We've provided a Python script (`docker_setup.py`) that automates the process of setting up a MySQL Docker container for this project.

### How to Use

1. Make sure Docker is running on your system.
2. Ensure your `.env` file is properly configured with the following variables:
   - `DB_USER`: MySQL username
   - `DB_PASS`: MySQL password
   - `DB_NAME`: Database name
   - `DB_PORT`: Port to expose MySQL (default: 3306)
3. Run the setup script:
   ```bash
   python docker_setup.py
   ```
4. The script will:
   - Check if Docker is available
   - Verify if the specified port is free
   - Remove any existing container with the name 'yt_mysql_local'
   - Start a new MySQL 8.0 container with your configuration
   - Wait for MySQL to be ready

### Troubleshooting

- **Docker not installed**: If you see "ERROR: Docker not installed or not responding", make sure Docker is installed and running.
- **Port in use**: If you see "ERROR: Port in use", choose a different port in your `.env` file or stop the service using that port.
- **Container start failed**: Check the error message for details. Common issues include insufficient permissions or conflicts with existing containers.

## Manual Setup (Alternative)

If you prefer to set up the MySQL container manually, you can use the following Docker command:

```bash
docker run -d --name yt_mysql_local \
  -e MYSQL_ROOT_PASSWORD=your_password \
  -e MYSQL_DATABASE=yt_proj \
  -e MYSQL_USER=etl_user \
  -e MYSQL_PASSWORD=your_password \
  -p 3306:3306 \
  mysql:8.0
```

Replace `your_password` with your actual password.

## Connecting to the MySQL Database

Once the container is running, you can connect to the MySQL database using:

```bash
docker exec -it yt_mysql_local mysql -u etl_user -p
```

You'll be prompted for the password you specified in the `.env` file.

## Stopping and Removing the Container

To stop the container:
```bash
docker stop yt_mysql_local
```

To remove the container:
```bash
docker rm yt_mysql_local
```
