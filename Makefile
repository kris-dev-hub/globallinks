include .env.example

GOFUMPT=gofumpt -l -w

.PHONY:
.DEFAULT_GOAL := help

#thanks to https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html

build: ## Builds the binary
	@echo "Building binary"
	go build -o bin/importer cmd/importer/main.go

lint: $(GOLANGCI) ## Runs golangci-lint with predefined configuration
	@echo "Applying linter"
	golangci-lint version
	golangci-lint run -c .golangci.toml ./...

help: ## Display this help screen
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

compose-up: ## Starts the docker-compose, example make compose-up ARCHIVENAME="CC-MAIN-2021-04"
	@echo "Starting docker-compose"
	ARCHIVENAME=${ARCHIVENAME} GLOBALLINKS_MAXWATFILES=${GLOBALLINKS_MAXWATFILES} GLOBALLINKS_MAXTHREADS=${GLOBALLINKS_MAXTHREADS} docker-compose up -d --build watimport

compose-down: ## Stops the docker-compose
	@echo "Stopping docker-compose"
	docker-compose down

docker-rm-volumes: ## Removes all docker volumes
	@echo "Removing all docker volumes"
	docker volume rm $$(docker volume ls -qf dangling=true)

docker-build: ## Builds the Docker image
	@echo "Building Docker image"
	docker build -t watlinksapp .

docker-start: ## Starts the Docker container
	@echo "Starting Docker container"
	docker run -d -v ./watdata:/app/data watlinksapp /app/importer $(ARCHIVENAME) $(GLOBALLINKS_MAXWATFILES) $(GLOBALLINKS_MAXTHREADS)

docker-stop: ## Stops the Docker container
	@echo "Stopping Docker container"
	docker stop $$(docker ps -q --filter ancestor=watlinksapp)