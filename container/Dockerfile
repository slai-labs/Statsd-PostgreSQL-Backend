# Sets up and runs statsd using the PostgreSQL backend

# Pull base image.
FROM node:4-slim
MAINTAINER Kris Siegel <kris@immuta.com>

RUN apt-get update; apt-get install -y --no-install-recommends git
ADD ./init.sh ./configuration.js /tmp/

ENV POSTGRES_HOST 0.0.0.0
ENV POSTGRES_USER kris
#ENV POSTGRES_PASSWORD
ENV POSTGRES_DATABASE postgres

RUN chmod +x /tmp/init.sh && sync && /tmp/init.sh
