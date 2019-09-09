from db_connect import connect_database, disconnect_database
from api_connect import APIConnector
import requests


def buildSymbolTable():
    db_connection = connect_database()
    db_cursor = db_connection.cursor()
    db_cursor.execute("DROP TABLE IF EXISTS symbols")
    db_cursor.execute("CREATE TABLE symbols ("
                      "symbol TEXT, "
                      "exchange TEXT, "
                      "name TEXT, "
                      "date DATE, "
                      "type TEXT, "
                      "region TEXT, "
                      "currency TEXT)")
    
    disconnect_database(db_cursor, db_connection)

def getSymbolData(production):
    apiConnector = APIConnector(production)
    db_connection = connect_database()
    db_cursor = db_connection.cursor()
    #apiResult = apiConnector.getAPIData("/ref-data/symbols")
    apiResult = apiConnector.getSymbolData()
    for result in apiResult :
        db_cursor.execute("INSERT INTO symbols ("
                          "symbol, "
                          "exchange, "
                          "name, "
                          "date, "
                          "type, "
                          "region, "
                          "currency) "
                          "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                          (result["symbol"],
                           result["exchange"],
                           result["name"],
                           result["date"],
                           result["type"],
                           result["region"],
                           result["currency"]))
        db_connection.commit()
        print(result)
    


    
    
if __name__ == "__main__" :
    production = False
    
    buildSymbolTable()
    getSymbolData(production)
    
