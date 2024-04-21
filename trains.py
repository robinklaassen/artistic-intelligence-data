import json
import os

import requests
from dotenv import load_dotenv

load_dotenv()

NS_VIRTUAL_TRAIN_URL = "https://gateway.apiportal.ns.nl/virtual-train-api/api/vehicle"
NS_API_KEY = os.getenv("NS_API_KEY")

if __name__ == "__main__":
    response = requests.get(NS_VIRTUAL_TRAIN_URL,
                            headers={'Ocp-Apim-Subscription-Key': NS_API_KEY},
                            timeout=10)
    response.raise_for_status()
    print(json.dumps(response.json(), indent=2))
