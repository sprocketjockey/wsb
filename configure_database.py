import psycopg2


def connect_database():
    return psycopg2.connect("dbname=economy_data "
                            "user=minion "
                            "password=jason "
                            "host=172.16.1.116 "
                            "port=5432")

def disconnect_database(db_cursor, db_connection):
    db_connection.commit()
    db_cursor.close()
    db_connection.close()

def configure_database(db_cursor):
    db_cursor.execute("DROP TABLE IF EXISTS sp500")
    db_cursor.execute("CREATE TABLE sp500 ("
                      "date date, "
                      "open numeric(12,2), "
                      "high numeric(12,2), "
                      "low numeric(12,2), "
                      "close numeric(12,2), "
                      "adj_close numeric(12,2), "
                      "volume bigint)")
    

def main():
    db_connection = connect_database()
    db_cursor = db_connection.cursor() 
    configure_database(db_cursor)
    disconnect_database(db_cursor, db_connection)

if __name__ == "__main__":
    main()
