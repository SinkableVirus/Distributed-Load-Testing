import requests

while True:
    res = requests.get("http://localhost:8080/ping")
    print(res.text)