from db_connect import connect_database, disconnect_database
from api_connect import APIConnector
import requests

def buildTimeSeriesTable():
    db_connection = connect_database()
    db_cursor = db_connection.cursor()
    db_cursor.execute("DROP TABLE IF EXISTS timeseries_year")
    db_cursor.execute("CREATE TABLE timeseries_year ("
                      "symbol TEXT, "
                      "date DATE, "
                      "high NUMERIC(10,2), "
                      "low NUMERIC(10,2), "
                      "volume INTEGER, "
                      "open NUMERIC(10,2), "
                      "close NUMERIC(10,2))")
    
    disconnect_database(db_cursor, db_connection)

def getTimeSeriesData(production):
    apiConnector = APIConnector(production)
    db_connection = connect_database()
    db_cursor = db_connection.cursor()
    symbolList = getSymbolList(db_cursor)

    for symbol in symbolList :
        print(symbol)
        ts = apiConnector.getYearTimeSeries(symbol)
        if (ts) :
            storeSymbolData(db_cursor,symbol, ts)
            db_connection.commit()
        else:
            print("Error: " + symbol)


def getSymbolList(db_cursor) :
    db_cursor.execute("SELECT symbol FROM symbols "
                      "WHERE type = 'cs' ORDER BY symbol")
    
    symbolTupleList = db_cursor.fetchall()
    symbolList = []
    for symbol in symbolTupleList:
        symbolList.append(symbol[0])

    return symbolList

def storeSymbolData(db_cursor, symbol, symbolData) :
    for data in symbolData:
        db_cursor.execute("INSERT INTO timeseries_year ("
                          "symbol, "
                          "date, "
                          "high, "
                          "low, "
                          "volume, "
                          "open, "
                          "close) "
                          "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                          (symbol,
                           data["date"],
                           data["high"],
                           data["low"],
                           data["volume"],
                           data["open"],
                           data["close"]))

if __name__ == "__main__" :
    production = False

    buildTimeSeriesTable()
    getTimeSeriesData(production)
