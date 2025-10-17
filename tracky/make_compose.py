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
    attachable: Optional[bool] = None


class ServiceSecret(BaseModel):
    source: str
    target: str


class ServiceConfig(BaseModel):
    image: Optional[str] = None
    environment: Optional[Dict[str, Union[str, int]]] = None
    networks: Optional[List[str]] = None
    secrets: Optional[List[ServiceSecret]] = None
    deploy: Optional[Dict] = None


class DockerCompose(BaseModel):
    services: Dict[str, ServiceConfig]
    secrets: Dict[str, SecretConfig]
    networks: Dict[str, NetworkConfig]


def create_compose(credentials: List[Dict]):
    compose = DockerCompose(
        services={},
        secrets={},
        networks={"trackynet": NetworkConfig(driver="overlay", attachable=True)},
    )

    for cred in credentials:
        regional = "true" if random.randint(1, 100) > 10 else "false"
        urban = "true" if random.randint(1, 100) > 50 and regional == "true" else "false"

        service_name = cred["id"]

        # Ensure dir exists + dump credential file
        os.makedirs(service_name, exist_ok=True)
        with open(f"{service_name}/tdevice.json", "w") as f:
            json.dump(cred, f, indent=2)

        # Register secrets
        tdevice_secret = f"{service_name}_tdevice"
        compose.secrets[tdevice_secret] = SecretConfig(file=f"./{service_name}/tdevice.json")

        # Define service with secrets mounted
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
                ServiceSecret(
                    source=tdevice_secret,
                    target="/app/credentials/tdevice.json",
                )
            ],
            deploy={
                "replicas": 1,
                "restart_policy": {"condition": "on-failure"},
            },
        )

    # Dump YAML correctly
    with open("docker-compose.yml", "w") as f:
        compose_dict = compose.model_dump(exclude_none=True)

        # yaml.safe_dump handles Pydantic dict fine
        yaml_str = yaml.safe_dump(compose_dict, sort_keys=False)
        f.write(yaml_str)


def token():
    response = requests.post(f"http://{sys.argv[1]}/login/", json={"username": "leonardo", "password": "subemelaradio"})
    return response.json()["token"]

def main():
    if len(sys.argv) != 2:
        print(f"Usage: python {sys.argv[0]} <backend_ip:port>")
        sys.exit(1)

    try:
        t = token()
        credentials = requests.get(f"http://{sys.argv[1]}/devices/credentials", headers={"Authorization": f"Bearer {t}"}).json()
    except Exception as e:
        print(f"Error fetching credentials: {e}")
        sys.exit(1)

    create_compose(sorted(credentials, key=lambda x: x["name"]))

if __name__ == "__main__":
    main()
