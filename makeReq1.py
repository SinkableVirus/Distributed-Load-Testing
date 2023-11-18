import requests
import time

while True:
    res = requests.get("http://localhost:8080/ping")
    print(res.text)