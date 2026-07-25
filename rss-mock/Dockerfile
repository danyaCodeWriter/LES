# syntax=docker/dockerfile:1
FROM python:3.12-alpine
WORKDIR /app
COPY server.py /app/server.py
ENV PORT=8090 \
    RECOVERY_GROUP=demo-rg \
    HOME_REGION=r1 \
    PEER_REGION=r2 \
    CYCLE_INTERVAL_SEC=8 \
    HEARTBEAT_INTERVAL_SEC=2
EXPOSE 8090
CMD ["python", "-u", "/app/server.py"]
