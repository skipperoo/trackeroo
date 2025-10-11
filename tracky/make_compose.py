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
    network_mode: Optional[str] = None
    restart: Optional[str] = None
    extra_hosts: Optional[List[str]] = None


class DockerCompose(BaseModel):
    services: Dict[str, ServiceConfig]
    volumes: Optional[Dict[str, VolumeConfig]] = None
    networks: Optional[Dict[str, NetworkConfig]] = None
    include: Optional[List[str]] = None

def load_credentials(path: str):
    with open(path, 'r') as file:
        return json.load(file)

def create_compose(credentials: List[Dict]):
    """
    Create a DockerCompose object from a dictionary of credentials.
    An example item of the credentials list is:
    {
      "id": "trk-5dac1f15577caca3",
      "name": "fancy_shockley",
      "device_type": "valuable",
      "private_key": "2mtRLJjIO0yB1UDtOzEVOpiBnAxebQagR7LvaM9MMLM=",
      "mqtt_host": "localhost",
      "mqtt_port": 1883,
      "mqtt_mode": "insecure",
      "ca_cert": ""
    }
    Args:
        credentials (Dict[str, Dict]): A dictionary of credentials.

    Returns:
        DockerCompose: A DockerCompose object.
    """
    compose = DockerCompose(services={} )#, include=["geo-services/docker-compose.yml"])
    print("SELECT * FROM ( VALUES ")
    for i, cred in enumerate(credentials):
        # print(f"Processing device {cred}")
        # continue
        compose.services[cred["id"]] = ServiceConfig(
            build={
                "context": "src",
                "dockerfile": "Dockerfile"
            },
            environment={
                "GEOCODING_SERVICE_URL": "http://localhost:8082",
                "ROUTING_SERVICE_URL": "http://localhost:5000",
                "OVERPASS_URL": "http://localhost:12345/api/interpreter",
                "REGIONAL": "true" if random.randint(1, 100) < 10 else "false",
                "PUBLISH_PERIOD": "500",
                "PIRATE": "true" if random.randint(1, 100) < 10 else "false"
            },
            # depends_on=["nominatim", "osrm"],
            network_mode="host",
            volumes=[f"./{cred["id"]}:/app/credentials"],
            restart="no",
        )
        # if compose.services[cred["id"]].environment["PIRATE"] == "true":
        #     print(f"{cred['id']} is a pirate 🏴‍☠️")
        os.makedirs(f"{cred['id']}", exist_ok=True)
        with open(f"{cred['id']}/tdevice.json", "w") as f:
            json.dump(cred, f, indent=2)
        if i == len(credentials) - 1:
            print(f"('{cred['name']}', '{cred['id']}')")
        else:
            print(f"('{cred['name']}', '{cred['id']}'),")
    print(") AS t (__text, __value)")
    with open("docker-compose.yml", "w") as f:
        compose_dict = compose.model_dump(exclude_none=True)

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
