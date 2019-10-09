import psycopg2
from db_connect import connect_database, disconnect_database


def buildSP500Table(db_connection):
        db_cursor = db_connection.cursor()
        db_cursor.execute("DROP TABLE IF EXISTS sp500")
        db_cursor.execute("CREATE TABLE sp500 ("
                          "symbol TEXT)")

        db_connection.commit()


def importSP500Symbols(db_cursor):
    with open('SP500.csv', 'r') as file:
        lines = file.readlines()
    for line in lines:
        symbol = line.split(',')[0]
        db_cursor.execute("INSERT INTO sp500 (symbol) VALUES (%s)", (symbol,))



if __name__ == "__main__" :
    db_connection = connect_database()

    buildSP500Table(db_connection)

    db_cursor = db_connection.cursor()

    importSP500Symbols(db_cursor)
    db_connection.commit()
    disconnect_database(db_cursor, db_connection)