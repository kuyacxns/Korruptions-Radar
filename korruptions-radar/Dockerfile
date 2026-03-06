ARG BUILD_FROM
FROM $BUILD_FROM

# Install dependencies
RUN apk add --no-cache python3 py3-pip nginx

# Install Python packages
RUN pip3 install flask --break-system-packages

# Copy application files
COPY rootfs /

# Make scripts executable
RUN chmod +x /usr/bin/run.sh

CMD ["/usr/bin/run.sh"]
