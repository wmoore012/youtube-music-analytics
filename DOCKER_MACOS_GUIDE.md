# Docker Setup Guide for macOS

## Troubleshooting Docker Connection Issues

If you encounter the following error when running `docker_setup.py`:
```
ERROR: Docker daemon not running (Docker Desktop needs to be started on macOS)
```

Follow these steps to resolve the issue:

1. **Open Docker Desktop application**
   - Find Docker Desktop in your Applications folder
   - You can also use Spotlight (Cmd+Space) and type "Docker"
   - Alternatively, run `open -a Docker` in Terminal

2. **Wait for Docker Desktop to fully start**
   - Look for the Docker icon in your menu bar to show it's running
   - This may take a minute or two on first launch

3. **Run the script again**
   ```
   python docker_setup.py
   ```

## Why This Happens

Docker on macOS requires Docker Desktop to be running before any Docker commands can work. The Docker daemon doesn't run automatically in the background on macOS like it might on Linux systems.

When you install Docker via Homebrew on macOS, it installs the Docker client tools, but the Docker daemon still runs through Docker Desktop.

## Additional Resources

- [Docker Desktop for Mac documentation](https://docs.docker.com/desktop/mac/)
- [Docker Desktop troubleshooting guide](https://docs.docker.com/desktop/troubleshoot/overview/)
