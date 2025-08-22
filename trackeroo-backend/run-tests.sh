#!/bin/bash

cleanup() {
    docker compose down
}
trap cleanup EXIT
docker compose up --build --abort-on-container-exit --exit-code-from trackeroo-backend-test
