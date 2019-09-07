from db_connect import configure_symbol_database
from api_connect import APIConnector
import requests


def buildSymbolTable():
    configure_symbol_database()


    
if __name__ == "__main__" :
    production = False
    apiConnector = APIConnector(production)
    apiResult = apiConnector.getAPIEndpoint("stock/aapl/chart/1y")
    print (apiResult)
    buildSymbolTable()
