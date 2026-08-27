# Tiny image that just runs topology.py once, then exits.
# compose runs this as a one-shot dependency before the services start.
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /srv
COPY libs/pyevents /srv/libs/pyevents
RUN pip install --no-cache-dir /srv/libs/pyevents
COPY infra/topology.py /srv/topology.py

CMD ["python", "topology.py"]
