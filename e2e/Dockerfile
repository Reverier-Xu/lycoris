# syntax=docker/dockerfile:1
FROM ubuntu:24.04

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY target/release/lycoris-daemon /usr/local/bin/lycoris-daemon
COPY target/release/lycoris /usr/local/bin/lycoris

RUN mkdir -p /etc/lycoris /var/lib/lycoris

ENTRYPOINT ["lycoris-daemon"]
CMD ["--config", "/etc/lycoris/config.toml"]
