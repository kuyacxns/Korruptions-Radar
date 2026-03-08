FROM ghcr.io/home-assistant/aarch64-base-python:3.11-alpine3.17

RUN apk add --no-cache py3-pip && pip3 install flask --break-system-packages

COPY rootfs /

RUN chmod +x /usr/bin/run.sh

CMD ["/usr/bin/run.sh"]
