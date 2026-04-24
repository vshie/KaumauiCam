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
    exfat-fuse \
    exfatprogs \
    ntfs-3g \
    psmisc \
    && rm -rf /var/lib/apt/lists/*

ARG GO2RTC_VERSION=1.9.9
ARG TARGETARCH=arm64
RUN set -eux; \
    case "$TARGETARCH" in \
      arm64) G2BIN="go2rtc_linux_arm64" ;; \
      arm) G2BIN="go2rtc_linux_arm" ;; \
      *) echo "Unsupported TARGETARCH=$TARGETARCH"; exit 1 ;; \
    esac; \
    curl -fsSL "https://github.com/AlexxIT/go2rtc/releases/download/v${GO2RTC_VERSION}/${G2BIN}" -o /usr/local/bin/go2rtc; \
    chmod +x /usr/local/bin/go2rtc

WORKDIR /app

COPY app/requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r /app/requirements.txt

COPY app/ /app/

RUN mkdir -p /app/data/recordings /app/data/in_progress

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
