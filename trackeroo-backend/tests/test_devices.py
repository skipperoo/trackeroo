import pytest
import requests
from fixtures.globals import URL
from fixtures.auth import gen_token

@pytest.fixture(scope="module")
def token():
    response = requests.post(f"{URL}/login/", json={"username": "leonardo", "password": "subemelaradio"})
    assert response.status_code == 200
    assert "token" in response.json()
    assert response.json()["token"] != ""
    return response.json()["token"]

def test_health():
    response = requests.get(f"{URL}/health")
    assert response.status_code == 200
    assert response.json() == {"details": "Healthy!"}


def test_crud_device(token):
    device = {
        "name": "Test Device",
        "device_type": "valuable"
    }
    # Create device
    response = requests.post(f"{URL}/devices/", headers={"Authorization": f"Bearer {token}"}, json=device)
    assert response.status_code == 201
    created_device_id = response.json()["id"]

    # Read device
    response = requests.get(f"{URL}/devices/{created_device_id}", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200
    assert response.json()["id"] == created_device_id
    assert response.json()["name"] == "Test Device"
    assert response.json()["device_type"] == "valuable"
    # Update - not implemented
    device = {
        "name": "Updated Device",
        "device_type": "valuable"
    }
    response = requests.put(f"{URL}/devices/{created_device_id}", headers={"Authorization": f"Bearer {token}"}, json=device)
    assert response.status_code == 501

    # Delete device
    response = requests.delete(f"{URL}/devices/{created_device_id}", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 204

    # Read device after deletion
    response = requests.get(f"{URL}/devices/{created_device_id}", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 404

def test_get_credentials(token):
    device = {
        "name": "Test Device Credentials",
        "device_type": "valuable"
    }
    # Create device
    response = requests.post(f"{URL}/devices/", headers={"Authorization": f"Bearer {token}"}, json=device)
    assert response.status_code == 201
    created_device_id = response.json()["id"]
    response = requests.get(f"{URL}/devices/{created_device_id}/credentials", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200
    assert response.json()["id"] == created_device_id
    assert response.json()["name"] == device["name"]
    assert response.json()["device_type"] == device["device_type"]
    assert response.json()["private_key"] is not None
    assert response.json()["mqtt_host"] == "localhost"
    assert response.json()["mqtt_port"] == 1883
    assert response.json()["mqtt_mode"] == "insecure"
    assert response.json()["ca_cert"] == ''

    response = requests.get(f"{URL}/devices/{created_device_id}/credentials", params={"secure": True}, headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200
    assert response.json()["id"] == created_device_id
    assert response.json()["name"] == device["name"]
    assert response.json()["device_type"] == device["device_type"]
    assert response.json()["private_key"] is not None
    assert response.json()["mqtt_host"] == "localhost"
    assert response.json()["mqtt_port"] == 8883
    assert response.json()["mqtt_mode"] == "secure"
    assert response.json()["ca_cert"] != ''

def test_auth_device(token):
    device = {
        "name": "Test Device",
        "device_type": "valuable"
    }
    # Create device
    response = requests.post(f"{URL}/devices/", headers={"Authorization": f"Bearer {token}"}, json=device)
    assert response.status_code == 201
    created_device_id = response.json()["id"]
    prv_key = response.json()["private_key"]
    password = gen_token(created_device_id, prv_key)
    response = requests.post(f"{URL}/auth/user", data={"username": created_device_id, "password": password})
    assert response.status_code == 200
    assert response.text == "allow"
    response = requests.post(f"{URL}/auth/topic", data={"username": created_device_id, "routing_key": f"/data/{created_device_id}"})
    assert response.status_code == 200
    assert response.text == "allow"
    response = requests.post(f"{URL}/auth/topic", data={"username": created_device_id, "routing_key": f"/data/subemelaradio"})
    assert response.status_code == 200
    assert response.text == "deny"
    password = gen_token("other_sub", prv_key)
    response = requests.post(f"{URL}/auth/user", data={"username": created_device_id, "password": password})
    assert response.status_code == 200
    assert response.text == "deny"
    response = requests.post(f"{URL}/auth/user", data={"username": created_device_id, "password": "wrong_password"})
    assert response.status_code == 200
    assert response.text == "deny"
