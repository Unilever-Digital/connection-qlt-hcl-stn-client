from app.controls.control import *
import pyodbc

def queryOptimization(table):
    connection = pyodbc.connect(driver="SQL Server",
                                    server='192.168.2.4',
                                    database='Vision_Mas140',
                                    uid='Control',
                                    pwd='123456',
                                    port=1433)
    cursor = connection.cursor()
    query = "SELECT * FROM {table} ORDER BY [ID] DESC LIMIT 100000"
    data = cursor.execute(query)
    print(data)

    counter_server = connectToMongoDB(table)
    counter_server.insert_many(data)

    connection.close()


if __name__ == "__main__":
    """
    tạm thời đưa data lên trước, sau cài mỗi ngày push 1 lầm
    """
    queryOptimization("Table_ResultCap")
    queryOptimization("Table_ResultCarton")
    queryOptimization("Table_ResultCounterBottle")