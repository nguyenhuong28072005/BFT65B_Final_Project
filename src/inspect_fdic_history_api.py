
import requests
import json

url = "https://api.fdic.gov/banks/history"

params = {
    "limit": 3,
    "format": "json"
}

print("Requesting FDIC History API...")

response = requests.get(
    url,
    params=params,
    timeout=60
)

print("HTTP status:", response.status_code)

response.raise_for_status()

result = response.json()

print("\nTOP-LEVEL JSON KEYS:")
print(list(result.keys()))

print("\nAPI METADATA:")
print(json.dumps(result.get("meta", {}), indent=2))

print("\nFIRST 3 HISTORY RECORDS:")
records = result.get("data", [])

print("Number of records returned:", len(records))

for index, record in enumerate(records, start=1):
    print(f"\nRECORD {index}:")
    print(json.dumps(record, indent=2, ensure_ascii=False))

print("\nCHECK COMPLETED")
