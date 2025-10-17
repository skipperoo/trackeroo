import os
import random
import sys
from typing import Dict, List, Optional, Union
from pydantic import BaseModel
import json
import requests
import yaml


class SecretConfig(BaseModel):
    file: str


class NetworkConfig(BaseModel):
    driver: Optional[str] = None


class ServiceConfig(BaseModel):
    image: Optional[str] = None
    environment: Optional[Dict[str, Union[str, int]]] = None
    networks: Optional[List[str]] = None
    secrets: Optional[List[Dict[str, str]]] = None
    deploy: Optional[Dict] = None


class DockerCompose(BaseModel):
    services: Dict[str, ServiceConfig]
    secrets: Dict[str, SecretConfig]
    networks: Dict[str, NetworkConfig]


def create_compose(credentials: List[Dict]):
    compose = DockerCompose(
        services={},
        secrets={},
        networks={"trackynet": NetworkConfig(driver="overlay")},
    )

    # Device-specific services
    for cred in credentials:
        regional = "true" if random.randint(1, 100) > 10 else "false"
        urban = "true" if random.randint(1, 100) > 50 and regional == "true" else "false"

        service_name = cred["id"]

        # ensure dir exists + dump files
        os.makedirs(service_name, exist_ok=True)
        with open(f"{service_name}/tdevice.json", "w") as f:
            json.dump(cred, f, indent=2)

        # register secrets
        tdevice_secret = f"{service_name}_tdevice"
        compose.secrets[tdevice_secret] = SecretConfig(file=f"./{service_name}/tdevice.json")

        # define service with secrets mounted
        compose.services[service_name] = ServiceConfig(
            image="tracky:latest",
            environment={
                "ROUTING_SERVICE_URL": "http://osrm:5000",
                "OVERPASS_URL": "http://nginx:80/api/interpreter",
                "REGIONAL": regional,
                "URBAN": urban,
                "PUBLISH_PERIOD": "2000",
                "PIRATE": "true" if random.randint(1, 100) < 10 else "false",
            },
            networks=["trackynet"],
            secrets=[
                {"source": tdevice_secret, "target": "/app/credentials/tdevice.json"},
            ],
            deploy={
                "replicas": 1,
                "restart_policy": {"condition": "on-failure"},
            },
        )

    # write compose file
    with open("docker-compose.yml", "w") as f:
        yaml_str = yaml.safe_dump(compose.model_dump(exclude_none=True), sort_keys=False)
        f.write(yaml_str)
