ARG BUILD_FROM=ghcr.io/home-assistant/aarch64-base-python:3.11-alpine3.17
FROM $BUILD_FROM

# Install system dependencies
RUN apk add --no-cache nginx

# Install Python packages
RUN pip3 install flask --break-system-packages

# Copy application files
COPY rootfs /

# Make scripts executable
RUN chmod +x /usr/bin/run.sh

CMD ["/usr/bin/run.sh"]
