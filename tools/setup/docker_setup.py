import os
import platform
import subprocess
import sys
import time
from pathlib import Path

from dotenv import load_dotenv


def run_docker_setup():
    # Load environment variables from .env file
    load_dotenv()

    # Get DB values from environment
    db_user = os.getenv("DB_USER", "")
    db_pass = os.getenv("DB_PASS", "")
    db_name = os.getenv("DB_NAME", "")
    db_port = os.getenv("DB_PORT", "3306")

    if not all([db_user, db_pass, db_name]):
        return "ERROR: Missing required database configuration in .env file"

    print(f"Using DB_USER={db_user}, DB_NAME={db_name}, host_port={db_port} (password hidden)")

    # Check Docker
    try:
        result = subprocess.run(["docker", "--version"], capture_output=True, text=True, timeout=10)
        if result.returncode != 0:
            return "ERROR: Docker not available"
        print(f"Docker available: {result.stdout.strip()}")

        # Check if Docker daemon is running
        daemon_check = subprocess.run(["docker", "info"], capture_output=True, text=True, timeout=10)
        if daemon_check.returncode != 0:
            is_macos = platform.system() == "Darwin"
            if is_macos and "Cannot connect to the Docker daemon" in daemon_check.stderr:
                print("\nERROR: Docker daemon is not running.")
                print("On macOS, you need to start Docker Desktop application.")
                print("1. Open Docker Desktop from your Applications folder")
                print("2. Wait for Docker Desktop to start completely")
                print("3. Run this script again\n")
                return "ERROR: Docker daemon not running (Docker Desktop needs to be started on macOS)"
            return f"ERROR: Docker daemon not running: {daemon_check.stderr}"
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return "ERROR: Docker not installed or not responding"

    # Check if port is in use
    try:
        result = subprocess.run(["lsof", "-i", f":{db_port}"], capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            print(f"PORT_IN_USE: port {db_port} seems in use")
            print(result.stdout[:200])  # Show first few lines
            return "ERROR: Port in use"
    except (subprocess.TimeoutExpired, FileNotFoundError):
        print("Port check skipped, proceeding...")

    # Remove existing container
    try:
        subprocess.run(["docker", "rm", "-f", "yt_mysql_local"], capture_output=True, timeout=15)
        print("Removed any existing yt_mysql_local container")
    except subprocess.TimeoutExpired:
        print("Container removal timed out")

    # Run MySQL container
    cmd = [
        "docker",
        "run",
        "-d",
        "--name",
        "yt_mysql_local",
        "-e",
        f"MYSQL_ROOT_PASSWORD={db_pass}",
        "-e",
        f"MYSQL_DATABASE={db_name}",
        "-e",
        f"MYSQL_USER={db_user}",
        "-e",
        f"MYSQL_PASSWORD={db_pass}",
        "-p",
        f"{db_port}:3306",
        "mysql:8.0",
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            error_msg = result.stderr
            if "Cannot connect to the Docker daemon" in error_msg and platform.system() == "Darwin":
                print("\nERROR: Cannot connect to Docker daemon.")
                print("On macOS, you need to start Docker Desktop application.")
                print("1. Open Docker Desktop from your Applications folder")
                print("2. Wait for Docker Desktop to start completely")
                print("3. Run this script again\n")
                return "ERROR: Container start failed: docker daemon not running (start Docker Desktop)"
            return f"ERROR: Container start failed: {error_msg}"
        print("MySQL container started, waiting for readiness...")
    except subprocess.TimeoutExpired:
        return "ERROR: Container start timed out"

    # Wait for MySQL to be ready
    for attempt in range(15):  # 30 seconds max
        try:
            ping_cmd = [
                "docker",
                "exec",
                "yt_mysql_local",
                "mysqladmin",
                "ping",
                "-h127.0.0.1",
                f"-p{db_pass}",
                "--silent",
            ]
            result = subprocess.run(ping_cmd, capture_output=True, timeout=5)
            if result.returncode == 0:
                print("MYSQL_READY")
                return "SUCCESS"
        except subprocess.TimeoutExpired:
            pass
        time.sleep(2)

    return "ERROR: MySQL not ready after 30 seconds"


if __name__ == "__main__":
    # Run the setup
    setup_result = run_docker_setup()
    print(f"Setup result: {setup_result}")
