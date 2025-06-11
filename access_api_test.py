import requests

print(requests.get("http://localhost:8001/columns/ProductApplication_ACES").json())

# print(requests.get("http://localhost:8001/tables").json())