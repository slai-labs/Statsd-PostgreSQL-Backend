FROM statsd/statsd:v0.10.1

RUN apt update && apt install -y netcat && apt clean \
    && npm install --global pg@^8 crypto-js nodemon

COPY . ../statsd-postgres-backend
RUN cd ../statsd-postgres-backend \
    && npm install

ADD statsdconfig.js config.js
