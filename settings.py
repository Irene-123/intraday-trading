import os 
#import json 

#BROKER_CREDENTIALS_FILE = "broker_credentials.json"

#broker_cred = {} 

#with open(BROKER_CREDENTIALS_FILE, "r") as file:
 #   broker_cred = json.load(file)




import json

# Specify the correct path to your broker_credentials.json file
BROKER_CREDENTIALS_FILE = "C:/Users/chand/Desktop/ntradpy/intraday-trading/broker_credentials.json"

try:
    with open(BROKER_CREDENTIALS_FILE, "r") as file:
        broker_cred = json.load(file)
except FileNotFoundError:
    print(f"Error: The file {BROKER_CREDENTIALS_FILE} was not found.")
    raise
except json.JSONDecodeError:
    print(f"Error: The file {BROKER_CREDENTIALS_FILE} could not be parsed.")
    raise
