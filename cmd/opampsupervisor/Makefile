include ../../Makefile.Common

e2e-test:
	make -C ../../ otelcontribcol
	go test -v --tags=e2e .

.PHONY: docker-build
docker-build:
	docker buildx build -t hny/opampsupervisor --platform linux/amd64,linux/arm64 .

.PHONY: docker-push
docker-push:
	AWS_PROFILE=sandbox-telemetry aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 017118846235.dkr.ecr.us-east-1.amazonaws.com
	AWS_PROFILE=sandbox-telemetry docker tag hny/opampsupervisor:latest 017118846235.dkr.ecr.us-east-1.amazonaws.com/hny/opampsupervisor:latest
	AWS_PROFILE=sandbox-telemetry docker push 017118846235.dkr.ecr.us-east-1.amazonaws.com/hny/opampsupervisor:latest