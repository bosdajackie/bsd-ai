import requests
import yaml
# print(requests.get("http://localhost:8001/columns/ProductApplication_ACES").json())

# print(requests.get("http://localhost:8001/tables").json())

schema = yaml.safe_load(open("db_schema.yaml")).get("tables", {})
print(schema)