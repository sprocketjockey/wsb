import json
import requests



def serverHelloWorld(server_config) :
    print("Server Hello World")
    url = server_config["base_url"] + "/stock/aapl/quote" + server_config["api_key"]

    getURL(url)

def timeSeries(server_config):
    print("Time Series options")
    url = server_config["base_url"] + "/time-series" + server_config["api_key"]
    print(url)
    #getURL(url)

def getURL(url):
    response = requests.get(url)
    print(response.json())

def loadServerData(name) :
    print("Loading Config File")
    with open(name, 'r') as fp:
        data = json.load(fp)
        return data

def setServerConfig(production) :
    all_configs = loadServerData("server_config.json")
    server_config = {}
    if (production) :
        server_config = all_configs["prod_server"]
    else :
        server_config = all_configs["test_server"]
    return server_config
    
if __name__ == "__main__":
    production = False

    server_config = setServerConfig(production)

    #serverHelloWorld(server_config)

    timeSeries(server_config)
