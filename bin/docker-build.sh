version=0.10.1

aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 133672401164.dkr.ecr.us-east-1.amazonaws.com
docker build --platform linux/amd64 -t beam-statsd:$version .

docker tag beam-statsd:$version 133672401164.dkr.ecr.us-east-1.amazonaws.com/beam-statsd:$version
docker tag beam-statsd:$version 133672401164.dkr.ecr.us-east-1.amazonaws.com/beam-statsd:latest

docker push 133672401164.dkr.ecr.us-east-1.amazonaws.com/beam-statsd:$version
docker push 133672401164.dkr.ecr.us-east-1.amazonaws.com/beam-statsd:latest
