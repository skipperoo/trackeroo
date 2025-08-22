import pytest
import requests

# URL = "http://trakeroo-backend:8081/"
URL = "http://localhost:8080"

def test_health():
    response = requests.get(f"{URL}/health")
    assert response.status_code == 200
    assert response.json() == {"details": "Healthy!"}

def test_login():
    response = requests.post(f"{URL}/login", json={"username": "leonardo", "password": "subemelaradio"})
    # assert response.status_code == 200
    print(response.json())
