FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-dev \
    ca-certificates \
    curl \
    ffmpeg \
    gstreamer1.0-tools \
    gstreamer1.0-plugins-base \
    gstreamer1.0-plugins-good \
    gstreamer1.0-plugins-bad \
    exfat-fuse \
    exfatprogs \
    ntfs-3g \
    iputils-ping \
    psmisc \
    && rm -rf /var/lib/apt/lists/*

ARG GO2RTC_VERSION=1.9.9
# `TARGETARCH` / `TARGETVARIANT` are auto-populated by buildx for multi-platform
# builds. Redeclare them WITHOUT a default — providing a default disables the
# auto-population in some buildx versions and was previously baking the arm64
# go2rtc binary into the linux/arm/v7 image (causing "Exec format error" on
# 32-bit Pis). We also fall back to `dpkg --print-architecture` when run via a
# plain `docker build`, which always reflects the running platform (the build
# RUN executes under QEMU emulation for the target arch, so dpkg returns the
# target's view).
ARG TARGETARCH
ARG TARGETVARIANT
RUN set -eux; \
    arch="${TARGETARCH:-$(dpkg --print-architecture)}"; \
    case "$arch" in \
      arm64)            G2BIN="go2rtc_linux_arm64" ;; \
      arm|armhf|armel)  G2BIN="go2rtc_linux_arm" ;; \
      amd64)            G2BIN="go2rtc_linux_amd64" ;; \
      *) echo "Unsupported arch='$arch' (TARGETARCH='$TARGETARCH' TARGETVARIANT='$TARGETVARIANT' dpkg='$(dpkg --print-architecture)')"; exit 1 ;; \
    esac; \
    echo "go2rtc: downloading $G2BIN for arch=$arch variant='${TARGETVARIANT:-}'"; \
    curl -fsSL "https://github.com/AlexxIT/go2rtc/releases/download/v${GO2RTC_VERSION}/${G2BIN}" -o /usr/local/bin/go2rtc; \
    chmod +x /usr/local/bin/go2rtc; \
    # Fail the build if we somehow shipped the wrong-arch binary: attempting to
    # exec it under the target's QEMU will return ENOEXEC ("Exec format error").
    /usr/local/bin/go2rtc -version 2>&1 | head -3

WORKDIR /app

COPY app/requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r /app/requirements.txt

COPY app/ /app/

# Offline UI fonts (no Google Fonts at runtime).
RUN mkdir -p /app/static/vendor/fonts && \
    curl -fsSL -o /app/static/vendor/fonts/dm-sans-400.woff2 \
      "https://cdn.jsdelivr.net/npm/@fontsource/dm-sans@5.2.5/files/dm-sans-latin-400-normal.woff2" && \
    curl -fsSL -o /app/static/vendor/fonts/dm-sans-500.woff2 \
      "https://cdn.jsdelivr.net/npm/@fontsource/dm-sans@5.2.5/files/dm-sans-latin-500-normal.woff2" && \
    curl -fsSL -o /app/static/vendor/fonts/dm-sans-600.woff2 \
      "https://cdn.jsdelivr.net/npm/@fontsource/dm-sans@5.2.5/files/dm-sans-latin-600-normal.woff2"

RUN curl -fsSL -o /app/static/vendor/vue.global.prod.js \
      "https://cdn.jsdelivr.net/npm/vue@3.5.13/dist/vue.global.prod.js"

RUN mkdir -p /app/data/recordings

ENV FLASK_APP=main.py
ENV PORT=6042
EXPOSE 6042
EXPOSE 8555

ARG IMAGE_NAME
LABEL permissions='\
{\
 "ExposedPorts": {\
  "6042/tcp": {},\
  "8555/tcp": {},\
  "8555/udp": {}\
 },\
 "HostConfig": {\
  "Binds": [\
   "/usr/blueos/extensions/kaumauicam:/app/data",\
   "/dev:/dev",\
   "/run/udev:/run/udev:ro"\
  ],\
  "NetworkMode": "host",\
  "Privileged": true,\
  "PortBindings": {\
   "6042/tcp": [{"HostPort": ""}]\
  }\
 }\
}'

ARG AUTHOR
ARG AUTHOR_EMAIL
LABEL authors='[{"name":"Tony White","email":"tonywhite@bluerobotics.com"}]'

ARG MAINTAINER
ARG MAINTAINER_EMAIL
LABEL company='{"name":"Blue Robotics","email":"support@bluerobotics.com"}'
LABEL type="tool"
ARG REPO
ARG OWNER
LABEL readme=''
LABEL links='{"source":"https://github.com/vshie/KaumauiCam"}'
LABEL requirements="core >= 1.1"

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
