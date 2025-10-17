import os
import random
import sys
from typing import Dict, List, Optional, Union
from pydantic import BaseModel
import json
import requests
import yaml


class VolumeConfig(BaseModel):
    driver: Optional[str] = None
    driver_opts: Optional[Dict[str, str]] = None
    external: Optional[Union[bool, Dict[str, str]]] = None


class NetworkConfig(BaseModel):
    driver: Optional[str] = None
    driver_opts: Optional[Dict[str, str]] = None
    external: Optional[Union[bool, Dict[str, str]]] = None


class ServiceConfig(BaseModel):
    image: Optional[str] = None
    build: Optional[Union[str, Dict[str, Union[str, Dict[str, str]]]]] = None
    command: Optional[Union[str, List[str]]] = None
    ports: Optional[List[str]] = None
    environment: Optional[Dict[str, Union[str, int]]] = None
    volumes: Optional[List[str]] = None
    depends_on: Optional[List[str]] = None
    networks: Optional[List[str]] = None
    restart: Optional[str] = None
    extra_hosts: Optional[List[str]] = None
    deploy: Optional[Dict] = None


class DockerCompose(BaseModel):
    services: Dict[str, ServiceConfig]
    volumes: Optional[Dict[str, VolumeConfig]] = None
    networks: Optional[Dict[str, NetworkConfig]] = None
    include: Optional[List[str]] = None


def load_credentials(path: str):
    with open(path, "r") as file:
        return json.load(file)


def create_compose(credentials: List[Dict]):
    compose = DockerCompose(
        services={},
        networks={"trackynet": NetworkConfig(driver="overlay")},
        volumes={}
    )

    # Generate per-device services
    for i, cred in enumerate(credentials):
        regional = "true" if random.randint(1, 100) > 10 else "false"
        urban = "true" if random.randint(1, 100) > 50 and regional == "true" else "false"

        cred_vol = f"{cred['id']}-credentials"
        data_vol = f"{cred['id']}-data"

        compose.services[cred["id"]] = ServiceConfig(
            image="tracky:latest",
            environment={
                "ROUTING_SERVICE_URL": "http://osrm:5000",
                "OVERPASS_URL": "http://nginx:80/api/interpreter",
                "REGIONAL": regional,
                "URBAN": urban,
                "PUBLISH_PERIOD": "2000",
                "PIRATE": "true" if random.randint(1, 100) < 10 else "false",
            },
            volumes=[f"{cred_vol}:/app/credentials", f"{data_vol}:/data"],
            networks=["trackynet"],
            restart="no",
            deploy={
                "replicas": 1,
                "restart_policy": {"condition": "on-failure"},
            },
        )

        # Register the volumes
        if compose.volumes:
            compose.volumes[cred_vol] = VolumeConfig()
            compose.volumes[data_vol] = VolumeConfig()

        # Optionally still write local files (for secrets/config)
        os.makedirs(f"{cred['id']}", exist_ok=True)
        with open(f"{cred['id']}/tdevice.json", "w") as f:
            json.dump(cred, f, indent=2)

    # Write compose to file
    with open("docker-compose.yml", "w") as f:
        compose_dict = compose.model_dump(exclude_none=True)
        yaml_str = yaml.safe_dump(compose_dict, sort_keys=False)
        f.write(yaml_str)


def token():
    response = requests.post(
        f"http://{sys.argv[1]}/login/",
        json={"username": "leonardo", "password": "subemelaradio"},
    )
    return response.json()["token"]


def main():
    if len(sys.argv) != 2:
        print(f"Usage: python {sys.argv[0]} <backend_ip:port>")
        sys.exit(1)

    try:
        t = token()
        credentials = requests.get(
            f"http://{sys.argv[1]}/devices/credentials",
            headers={"Authorization": f"Bearer {t}"},
        ).json()
    except Exception as e:
        print(f"Error fetching credentials: {e}")
        sys.exit(1)

    create_compose(sorted(credentials, key=lambda x: x["name"]))


if __name__ == "__main__":
    main()
