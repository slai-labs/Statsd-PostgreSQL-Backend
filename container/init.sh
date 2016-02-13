#!/bin/bash

echo "Cloning statsd git repository";
cd /opt/;
git clone https://github.com/etsy/statsd.git;
cd ./statsd/;

echo "Installing statsd dependencies";
npm install;

echo "Cloning statsd-postgresql-backend";
cd ./backends/;
git clone https://github.com/immuta/Statsd-PostgreSQL-Backend.git;
cd ./Statsd-PostgreSQL-Backend/;

echo "Installing statsd-postgresql-backend dependencies";
npm install;
cd ../../;

echo "Moving configuration";
mv /tmp/configuration.js /opt/statsd/configuration.js;

echo "Starting statsd";
node stats.js configuration.js
