build:
	okteto build -f ./Dockerfile -t okteto.dev/beam-statsd:latest

start:
	okteto up -f ./okteto.yml