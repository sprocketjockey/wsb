import json
import requests



def serverHelloWorld(server_config) :
    print("Server Hello World")
    url = server_config["base_url"] + "stock/aapl/quote"
    url = url + server_config["api_key"]

    print(url)
    getURL(url)

def timeSeries(server_config):
    print("Time Series options")
    url = server_config["base_url"] + "stock/aapl/chart/1y"
    url = url + server_config["api_key"]
    
    print(url)
    getURL(url)


def getSymbols(server_config):
    print("Symbols")
    url = server_config["base_url"] + "ref-data/symbols"
    url = url + server_config["api_key"]

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

    #timeSeries(server_config)

    getSymbols(server_config)
