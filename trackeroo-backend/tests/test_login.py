import pytest
import requests
from fixtures.globals import URL

def test_health():
    response = requests.get(f"{URL}/health")
    assert response.status_code == 200
    assert response.json() == {"details": "Healthy!"}

def test_login():
    response = requests.post(f"{URL}/login/", json={"username": "leonardo", "password": "subemelaradio"})
    assert response.status_code == 200
    assert "token" in response.json()
    assert response.json()["token"] != ""
