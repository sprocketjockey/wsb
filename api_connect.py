import json
import requests

class APIConnector():
    server_config = {}
    
    def __init__(self, production):
        self.setServerConfig(production)


    def setServerConfig(self, production):
        all_configs = self.loadServerData("server_config.json")
        if (production) :
            self.server_config = all_configs["prod_server"]
        else:
            self.server_config = all_configs["test_server"]

    def loadServerData(self, name) :
        with open(name, 'r') as fp:
            data = json.load(fp)
            return data

    def generateURL(self, end_point) :
        url = self.server_config["base_url"] + end_point
        url = url + self.server_config["api_key"]
        return url

    def getURL(self, url):
        response = requests.get(url)
        return response.json()
    
    def getAPIData(self, end_point):
        url = self.generateURL(end_point)
        return self.getURL(url)

    def getSymbolData(self) :
        url = self.generateURL("/ref-data/symbols")
        return self.getURL(url)
