from statsd/statsd:latest

COPY . ../statsd-postgres-backend

RUN npm install pg crypto-js

ADD statsdconfig.js config.js