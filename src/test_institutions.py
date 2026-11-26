
import requests

# FDIC Institutions API
url = "https://api.fdic.gov/banks/institutions"

params = {
    "fields": "CERT,STNAME,ACTIVE,CHANGEC1",
    "limit": 3,
    "format": "json"
}

try:
    response = requests.get(
        url,
        params=params,
        timeout=60
    )

    response.raise_for_status()
    result = response.json()

    print("=" * 50)
    print("FDIC INSTITUTIONS API TEST")
    print("=" * 50)

    print("\nAPI parameters:")
    print(result["meta"]["parameters"])

    print("\nTotal matching records:")
    print(result["meta"]["total"])

    print("\nNumber of records returned:")
    print(len(result["data"]))

    print("\nSample records:")

    for item in result["data"]:
        print(item["data"])

except requests.exceptions.RequestException as error:
    print("Connection error:", error)

except (ValueError, KeyError) as error:
    print("Data processing error:", error)
