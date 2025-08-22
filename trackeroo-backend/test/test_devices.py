# import pytest
# import requests

# # URL = "http://trakeroo-backend:8081/"
# URL = "http://localhost:8080"

# def test_health():
#     response = requests.get(f"{URL}/health")
#     assert response.status_code == 200
#     assert response.json() == {"details": "Healthy!"}

# def test_create_device():
#     device = {
#         "name": "Test Device",
#         "type": "valuable"
#     }
#     response = requests.post(f"{URL}/devices", json=device)
#     assert response.status_code == 201
#     print(response.json())
