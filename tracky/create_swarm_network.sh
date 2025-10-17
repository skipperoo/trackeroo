#!/bin/bash

docker network rm tracky

docker network create \
  --driver overlay \
  --attachable \
  tracky
