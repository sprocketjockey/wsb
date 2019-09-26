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
        try:
            return response.json()
        except json.decoder.JSONDecodeError:
            print(url)
            pass
    
    def getAPIData(self, end_point):
        url = self.generateURL(end_point)
        return self.getURL(url)

    def getSymbolData(self) :
        url = self.generateURL("/ref-data/symbols")
        return self.getURL(url)
    
    def getDailyTimeSeries(self, symbol) : 
        end_point = "/stock/" + symbol + "/chart/5y"
        url = self.generateURL(end_point)
        return self.getURL(url)

    def getYearTimeSeries(self, symbol):
        end_point = "/stock/" + symbol + "/chart/1y"
        url = self.generateURL(end_point)
        return self.getURL(url)

    def getMonthTimeSeries(self, symbol):
        end_point = "/stock/" + symbol + "/chart/1mm"
        url = self.generateURL(end_point)
        #print(url)
        return self.getURL(url)
    
