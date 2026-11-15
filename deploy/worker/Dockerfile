# Build with an explicit pulled registry digest, never a floating production tag.
ARG BASE_IMAGE
FROM ${BASE_IMAGE} AS dependencies
ARG DEBIAN_MIRROR=http://deb.debian.org/debian
ARG DEBIAN_SECURITY_MIRROR=http://deb.debian.org/debian-security
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN sed -i "s#http://deb.debian.org/debian-security#${DEBIAN_SECURITY_MIRROR}#g; s#http://deb.debian.org/debian#${DEBIAN_MIRROR}#g" /etc/apt/sources.list.d/debian.sources \
    && apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr tesseract-ocr-eng tesseract-ocr-chi-sim \
    libreoffice-writer libreoffice-impress poppler-utils \
    fonts-dejavu-core fonts-wqy-zenhei fonts-noto-cjk chromium ffmpeg \
    && dpkg-query -W > /opt/groupagent-os-packages.txt \
    && rm -rf /var/lib/apt/lists/*
COPY deploy/worker/requirements.txt /opt/groupagent-requirements.txt
ARG PYPI_INDEX_URL=https://pypi.org/simple
RUN python -m pip install --index-url "${PYPI_INDEX_URL}" --no-cache-dir --require-hashes -r /opt/groupagent-requirements.txt
RUN apt-get update && apt-get install -y --no-install-recommends libatomic1 \
    && dpkg-query -W > /opt/groupagent-os-packages.txt \
    && rm -rf /var/lib/apt/lists/*
FROM dependencies AS worker
LABEL org.groupagent.worker-protocol="1"
COPY src/groupagent /usr/local/lib/python3.13/site-packages/groupagent
COPY src/groupagent/sandbox/supervisor.py /opt/groupagent-worker.py
ENTRYPOINT ["/usr/local/bin/python3", "-I", "/opt/groupagent-worker.py"]
