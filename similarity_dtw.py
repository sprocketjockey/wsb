import psycopg2
import numpy as np
from scipy.spatial.distance import cdist
import math
import multiprocessing
import time
from db_connect import connect_database, disconnect_database
from numba import jit

def calculateDistance(nodeA, nodeB):
    xlen = len(nodeA)
    ylen = len(nodeB)
    x = np.array(nodeA).reshape(-1,1)
    y = np.array(nodeB).reshape(-1,1)
    dist_base = np.zeros((xlen + 1, ylen + 1))
    dist_base[0, 1:] = np.inf
    dist_base[1:, 0] = np.inf
    dist_view = dist_base[1:, 1:]
    dist_base[1:, 1:] = cdist(x, y, 'euclidean')
    distance = fastCalculateDistance(xlen, ylen, dist_base, dist_view)
    return distance
	# for i in range(xlen):
	# 	for j in range(ylen):
	# 		dist_view[i, j] = dist_view[i,j] + min(dist_base[i,j], dist_base[i, j+1], dist_base[i+1,j])
	
	# distance = dist_view[-1, -1]/math.sqrt(math.pow(dist_view.shape[0],2) + math.pow(dist_view.shape[1],2))
	

@jit (nopython=True)
def fastCalculateDistance(xlen, ylen, dist_base, dist_view):
    for i in range(xlen):
        for j in range(ylen):
            dist_view[i, j] = dist_view[i,j] + min(dist_base[i,j], dist_base[i, j+1], dist_base[i+1,j])
    
    distance = dist_view[-1, -1]/math.sqrt(math.pow(dist_view.shape[0],2) + math.pow(dist_view.shape[1],2))
    return distance


def getPriceData(symbol, db_cursor):
    start_time = time.time()
    db_cursor.execute("SELECT price_array FROM close_cache WHERE symbol = %s", (symbol,))
    results = db_cursor.fetchall()
    priceList = results[0][0]
    #print(priceList)
    end_time = time.time()
    elapsed = end_time - start_time
    return priceList

def getBulkPriceData(db_cursor):
    db_cursor.execute("SELECT DISTINCT symbol, array_agg(close ORDER BY date) FROM timeseries_daily GROUP BY symbol HAVING count(date)= 1258;")
    results = db_cursor.fetchall()
    priceData = {}
    for result in results:
        priceData[result[0]] = result[1]
    
    return priceData

def getSymbolList(db_cursor):
    db_cursor.execute("SELECT DISTINCT symbol FROM timeseries_daily GROUP BY symbol HAVING count(date) = 1258;")
    results = db_cursor.fetchall()
    
    symbolList = []
    for result in results:
        symbolList.append(result[0])

    symbolList.append(None)
    return symbolList

def similarityWorker(symbolList, priceData):
    db_conn = connect_database()
    db_cursor = db_conn.cursor()
    
    while True:
        active_symbol = symbolQueue.get()
        
        if active_symbol != None:
            a = priceData[active_symbol]
            print(active_symbol)
            for symbol in symbolList :
                if symbol != None:
                    #print(active_symbol + " " + symbol)
                    b = priceData[symbol]
                    distance = calculateDistance(a,b)
                    data = (active_symbol, symbol, distance)
                    storeSimilarity(db_cursor, data)
                    db_conn.commit()
        else:
            symbolQueue.put(None)
            db_conn.close()
            break

def buildSimilarityTable(db_connection):
        db_cursor = db_connection.cursor()
        db_cursor.execute("DROP TABLE IF EXISTS dtw_similarity")
        db_cursor.execute("CREATE TABLE dtw_similarity ("
                          "symbol_a TEXT, "
                          "symbol_b TEXT, "
                          "dtw_close_dist DOUBLE PRECISION)")

        db_connection.commit()

def storeSimilarity(db_cursor, data):
    db_cursor.execute("INSERT INTO dtw_similarity (symbol_a, symbol_b, dtw_close_dist) VALUES (%s, %s, %s)",(data[0], data[1], data[2]))
        
if __name__ == "__main__" :
    db_connection = connect_database()

    buildSimilarityTable(db_connection)

    db_cursor  = db_connection.cursor()


    priceData = getBulkPriceData(db_cursor)
    symbolQueue = multiprocessing.Queue()

    #symbolList = getSymbolList(db_cursor)

    symbolList = priceData.keys()

    for symbol in symbolList:
        symbolQueue.put(symbol)

    workers = []

    for i in range(16):
        p = multiprocessing.Process(target = similarityWorker, args=(symbolList, priceData))
        workers.append(p)
        p.start()

    for p in workers:
        p.join()

    disconnect_database(db_cursor, db_connection)

