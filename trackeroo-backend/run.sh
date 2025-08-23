#!/bin/bash

cleanup() {
    docker compose down
}
trap cleanup EXIT
docker compose up --build -d && docker compose logs -f trackeroo-backend mongo
