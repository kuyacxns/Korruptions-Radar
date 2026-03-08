FROM ghcr.io/home-assistant/aarch64-base-python:3.11-alpine3.17

RUN apk add --no-cache py3-flask py3-werkzeug py3-jinja2 py3-click py3-itsdangerous py3-markupsafe

COPY rootfs /

RUN chmod +x /usr/bin/run.sh

CMD ["/usr/bin/run.sh"]
