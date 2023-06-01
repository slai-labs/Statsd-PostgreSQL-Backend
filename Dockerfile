from statsd/statsd:latest

COPY . ../statsd-postgres-backend

RUN npm install pg@^8 crypto-js

ADD statsdconfig.js config.js