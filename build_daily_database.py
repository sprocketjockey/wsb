from db_connect import connect_database, disconnect_database
from api_connect import APIConnector
from io import StringIO
import requests
import multiprocessing
import random
import time


def buildTimeSeriesTable():
    db_connection = connect_database()
    db_cursor = db_connection.cursor()
    db_cursor.execute("DROP TABLE IF EXISTS timeseries_daily")
    db_cursor.execute("CREATE TABLE timeseries_daily ("
                      "symbol TEXT, "
                      "date DATE, "
                      "high NUMERIC(10,2), "
                      "low NUMERIC(10,2), "
                      "volume INTEGER, "
                      "open NUMERIC(10,2), "
                      "close NUMERIC(10,2))")
    
    disconnect_database(db_cursor, db_connection)

# def getTimeSeriesData(production):
#     apiConnector = APIConnector(production)
#     db_connection = connect_database()
#     db_cursor = db_connection.cursor()
#     symbolList = getSymbolList(db_cursor)

#     for symbol in symbolList :
#         getSymbolData(apiConnector, db_connection, symbol)


def getSymbolData(apiConnector) :
    run = True

    db_connection = connect_database()

    while run :
        
        symbol = symbolList.get()
        
        if symbol == None:
            print("Breaking")
            symbolList.put(None)
            break
        else:
            start_time = time.time()
            db_cursor = db_connection.cursor()
            print(symbol)
            ts = apiConnector.getDailyTimeSeries(symbol)
            
            if (ts) :
                storeSymbolData(db_cursor, symbol, ts)
                db_connection.commit()
            else:
                print("Error: " + symbol)
            end_time = time.time()
            elapsed_time = end_time - start_time
            #print(elapsed_time)


def getSymbolList(db_cursor) :
    db_cursor.execute("SELECT symbol FROM symbols WHERE type = 'cs' OR  type = 'et' OR type = 'cef' ORDER BY symbol")
    
    symbolTupleList = db_cursor.fetchall()
    symbolList = multiprocessing.Queue()
    for symbol in symbolTupleList:
        symbolList.put(symbol[0])

    return symbolList

def storeSymbolData(db_cursor, symbol, symbolData) :

    symbolResults = StringIO()

    for data in symbolData:
        resultLine = symbol + ',' + data['date'] + ',' + str(data['high']) + ',' + str(data['low']) + ',' + str(data['volume'])
        resultLine = resultLine + ',' + str(data['open']) + ',' + str(data['close']) + '\n'
        symbolResults.write(resultLine)
    
    symbolResults.seek(0)

    db_cursor.copy_from(symbolResults,'timeseries_daily', columns=('symbol', 'date', 'high', 'low', 'volume', 'open', 'close'), sep=',')

    # for data in symbolData:
    #     db_cursor.execute("INSERT INTO timeseries_daily ("
    #                       "symbol, "
    #                       "date, "
    #                       "high, "
    #                       "low, "
    #                       "volume, "
    #                       "open, "
    #                       "close) "
    #                       "VALUES (%s,%s,%s,%s,%s,%s,%s)",
    #                       (symbol,
    #                        data["date"],
    #                        data["high"],
    #                        data["low"],
    #                        data["volume"],
    #                        data["open"],
    #                        data["close"]))

if __name__ == "__main__" :
    production = True
    buildTimeSeriesTable()
    apiConnector = APIConnector(production)
    db_connection = connect_database()
    db_cursor = db_connection.cursor()
    symbolList = getSymbolList(db_cursor)

    workers = []
    symbolList.put(None)

    for i in range(4):
        p = multiprocessing.Process(target = getSymbolData, args=(apiConnector,))
        workers.append(p)
        p.start()
    
    for p in workers:
        p.join()

