from statsd/statsd:latest

COPY . ../statsd-postgres-backend

RUN cd .. \
    && cd statsd-postgres-backend \
    && npm install pg@^8

ADD statsdconfig.js config.js