build:
	okteto build -f ./Dockerfile -t okteto.dev/beam-statsd:latest

stop: 
	okteto down -f ./okteto.yml

start:
	okteto up -f ./okteto.yml