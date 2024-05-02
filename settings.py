import os 
import json 

BROKER_CREDENTIALS_FILE = "broker_credentials.json"

broker_cred = {} 

with open(BROKER_CREDENTIALS_FILE, "r") as file:
    broker_cred = json.load(file)
