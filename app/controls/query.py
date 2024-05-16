import datetime
import time
from datetime import datetime as dt
from app.controls.control import *

def optimizationQueryCheckweigher(table):
    """
    counter bottles server
    """
    connection = connectToSqlServer('DESKTOP-M7H8BIL', 'U-CheckDate-Barcode')
    cursor = connection.cursor()
    collection = ensure_collection_exists("U-CheckDate-Barcode-Po2",table)
    
    result = collection.delete_many({})
    startdate = datetime.datetime(2023, 1, 1, 0, 0, 0)
    query = f"""
    SELECT CONVERT(date, DateTime) AS date,
        DAY(DateTime) AS day,
        MONTH(DateTime) AS month,
        YEAR(DateTime) AS year,
        Shift AS shift,
		FGsCode AS sku,
		Line As line,
        Weight_Under AS under,
        Weight_Target AS target,
        Weight_Over AS over,
        COUNT(*) AS count,
        SUM(CASE WHEN Status = 'Good' THEN 1 ELSE 0 END) AS countGood,
        SUM(CASE WHEN Status = 'Not Good' THEN 1 ELSE 0 END) AS countNotgood
    FROM {table}
    GROUP BY CONVERT(date, DateTime), DAY(DateTime), MONTH(DateTime), YEAR(DateTime), Line, FGsCode, Weight_Under, Weight_Target, Weight_Over
            CASE
                WHEN DATEPART(hh, DateTime) < 6 THEN 1
                WHEN DATEPART(hh, DateTime) < 14 THEN 2
                ELSE 3
            END
    ORDER BY date, shift;
    """
    cursor.execute(query)
    data = cursor.fetchall()
    data_insert = []
    for row in data:
        new_row = {
            "date": row[0],
            "day": row [1],
            "month": row[2],
            "year": row[3],
            "shift": row[4],
            "sku":  row[5],
            "line": row[6],
            "under": row[7],
            "target": row[8],
            "over": row[9],
            "count": row[10],
            "countPass": row[11],
            "countNotgood": row[12],
        }
        data_insert.append(new_row)
    print(data_insert)

    collection.insert_many(data_insert)
    connection.close()

def optimizationQueryImageFail(table):
    """
    counter bottles server
    """
    connection = connectToSqlServer('DESKTOP-M7H8BIL', 'U-CheckDate-Barcode')
    cursor = connection.cursor()
    collection = ensure_collection_exists("U-CheckDate-Barcode-Po2", table)
    
    result = collection.delete_many({})
    
    startdate = datetime.datetime(2023, 1, 1, 0, 0, 0)
    pipeline = f"""
    SELECT CONVERT(date, DateTime) AS date,
        DAY(DateTime) AS day,
        MONTH(DateTime) AS month,
        YEAR(DateTime) AS year,
        CASE
            WHEN DATEPART(hh, DateTime) < 6 THEN 1
            WHEN DATEPART(hh, DateTime) < 14 THEN 2
            ELSE 3
        END AS shift,
		FGsCode AS sku,
		Line As line,
        BarcodeTarget AS target,
        BarcodeCurrent AS current,
        COUNT(*) AS count,
        COUNT(ImageFail) AS countImageFail
    FROM {table}
    GROUP BY CONVERT(date, DateTime), DAY(DateTime), MONTH(DateTime), YEAR(DateTime), Line, FGsCode, BarcodeTarget, BarcodeCurrent
            CASE
                WHEN DATEPART(hh, DateTime) < 6 THEN 1
                WHEN DATEPART(hh, DateTime) < 14 THEN 2
                ELSE 3
            END
    ORDER BY date, shift;
    """
    cursor.execute(pipeline)
    group_data = cursor.fetchall()
    data_insert = []
    for row in group_data:
        new_row = {
            "date": row[0],
            "day": row [1],
            "month": row[2],
            "year": row[3],
            "shift": row[4],
            "sku":  row[5],
            "line": row[6],
            "target": row[7],
            "current": row[8],
            "count": row[9],
            "countImageFail": row[10],
        }
        data_insert.append(new_row)

    collection.insert_many(data_insert)
    connection.close()

def optimizationQueryOperating(table):
    """
    counter bottles server
    """
    connection = connectToSqlServer('DESKTOP-M7H8BIL', 'U-CheckDate-Barcode')
    cursor = connection.cursor()
    collection = ensure_collection_exists("U-CheckDate-Barcode-Po2", table)

    result = collection.delete_many({})
    print(result)

    startdate = datetime.datetime(2023, 1, 1, 0, 0, 0)
    # Define the pipeline string using f-strings for cleaner formatting
    pipeline = f"""
    SELECT CONVERT(date, DateTime) AS date,
        DAY(DateTime) AS day,
        MONTH(DateTime) AS month,
        YEAR(DateTime) AS year,
        CASE
            WHEN DATEPART(hh, DateTime) < 6 THEN 1
            WHEN DATEPART(hh, DateTime) < 14 THEN 2
            ELSE 3
        END AS shift,
        FGsCode AS sku,
        COUNT(*) AS count,
        SUM(CASE WHEN Status = 'Good' THEN 1 ELSE 0 END) AS countPass,
        SUM(CASE WHEN Status = 'NotGood' THEN 1 ELSE 0 END) AS countReject
    FROM Table_ResultCounterBottles
    GROUP BY CONVERT(date, DateTime), DAY(DateTime), MONTH(DateTime), YEAR(DateTime), FGsCode,
        CASE
            WHEN DATEPART(hh, DateTime) < 6 THEN 1
            WHEN DATEPART(hh, DateTime) < 14 THEN 2
            ELSE 3
        END
    ORDER BY date, shift;
    """
    cursor.execute(pipeline)
    group_data = cursor.fetchall()

    data_insert = []
    for row in group_data:
        new_row = {
            "date": row[0],
            "day": row[1],
            "month": row[2],
            "year": row[3],
            "shift": row[4],
            "sku":  row[5],
            "count": row[6],
            "countGood": row[7],
            "countNotGood": row[8],
        }
        # for key, value in new_row.items():
        # if key == "date":
        # new_row[key] = dt.strftime(value, "%Y-%m-%d")
        # else:
        # new_row[key] = value if value != float('nan') else ""
        data_insert.append(new_row)

    collection.insert_many(data_insert)
    connection.close()

def optimizationQueryProduct(table):
    """
    counter bottles server
    """
    connection = connectToSqlServer('DESKTOP-M7H8BIL', 'U-CheckDate-Barcode')
    cursor = connection.cursor()
    collection = ensure_collection_exists("U-CheckDate-Barcode-Po2", table)
    
    result = collection.delete_many({})
    print(result)

    startdate = datetime.datetime(2023, 1, 1, 0, 0, 0)
    # Define the pipeline string using f-strings for cleaner formatting
    pipeline = f"""
    SELECT CONVERT(date, DateTime) AS date,
        DAY(DateTime) AS day,
        MONTH(DateTime) AS month,
        YEAR(DateTime) AS year,
        CASE
            WHEN DATEPART(hh, DateTime) < 6 THEN 1
            WHEN DATEPART(hh, DateTime) < 14 THEN 2
            ELSE 3
        END AS shift,
        FGsCode AS sku,
        COUNT(*) AS count,
        SUM(CASE WHEN Status = 'Good' THEN 1 ELSE 0 END) AS countPass,
        SUM(CASE WHEN Status = 'NotGood' THEN 1 ELSE 0 END) AS countReject
    FROM {table}
    GROUP BY SKUID
    ORDER BY date, shift;
    """
    cursor.execute(pipeline)
    group_data = cursor.fetchall()

    data_insert = []
    for row in group_data:
        new_row = {
            "date": row[0],
            "day": row [1],
            "month": row[2],
            "year": row[3],
            "shift": row[4],
            "sku":  row[5],
            "count": row[6],
            "countGood": row[7],
            "countNotGood": row[8],
        }
        #for key, value in new_row.items():
            #if key == "date":
                #new_row[key] = dt.strftime(value, "%Y-%m-%d")
            #else:
                #new_row[key] = value if value != float('nan') else ""
        data_insert.append(new_row)

    collection.insert_many(data_insert)
    connection.close()

def optimizationQueryResultCarton(table):
    """
    result carton
    """
    connection = connectToSqlServer('DESKTOP-M7H8BIL', 'U-CheckDate-Barcode')
    cursor = connection.cursor()
    collection = ensure_collection_exists("U-CheckDate-Barcode-Po2", table)
    
    result = collection.delete_many({})
    startdate = datetime.datetime(2023, 1, 1, 0, 0, 0)
    query = f"""
    SELECT CONVERT(date, DateTime) AS date,
        DAY(DateTime) AS day,
        MONTH(DateTime) AS month,
        YEAR(DateTime) AS year,
        Shift AS shift,
		FgsCode AS sku,
        ProductName AS productName,
		Line As line,
        COUNT(*) AS count,
        SUM(CASE WHEN Status = 'Good' THEN 1 ELSE 0 END) AS countGood,
        SUM(CASE WHEN Status = 'Not Good' THEN 1 ELSE 0 END) AS countNotgood
    FROM {table}
    GROUP BY CONVERT(date, DateTime), DAY(DateTime), MONTH(DateTime), YEAR(DateTime), Line, FGsCode, ProductName
        CASE
            WHEN DATEPART(hh, DateTime) < 6 THEN 1
            WHEN DATEPART(hh, DateTime) < 14 THEN 2
            ELSE 3
        END
    ORDER BY date, shift;
    """
    cursor.execute(query)
    data = cursor.fetchall()
    data_insert = []
    for row in data:
        new_row = {
            "date": row[0],
            "day": row[1],
            "month": row[2],
            "year": row[3],
            "shift": row[4],
            "sku":  row[5],
            "name": row[6],
            "line": row[7],
            "count": row[8],
            "countGood": row[9],
            "countNotgood": row[10],
        }
        data_insert.append(new_row)
    print(data_insert)

    collection.insert_many(data_insert)
    connection.close()

def optimizationQueryResultDataman(table):
    """
    result dataman
    """
    connection = connectToSqlServer('DESKTOP-M7H8BIL', 'U-CheckDate-Barcode')
    cursor = connection.cursor()
    collection = ensure_collection_exists("U-CheckDate-Barcode-Po2", table)
    
    result = collection.delete_many({})
    startdate = datetime.datetime(2023, 1, 1, 0, 0, 0)
    query = f"""
    SELECT CONVERT(date, DateTime) AS date,
        DAY(DateTime) AS day,
        MONTH(DateTime) AS month,
        YEAR(DateTime) AS year,
        Shift AS shift,
		FGsCode AS sku,
        ProductName AS productName,
		Line As line,
        COUNT(*) AS count,
        SUM(CASE WHEN Status = 'Good' THEN 1 ELSE 0 END) AS countGood,
        SUM(CASE WHEN Status = 'Not Good' THEN 1 ELSE 0 END) AS countNotgood
    FROM {table}
    GROUP BY CONVERT(date, DateTime), DAY(DateTime), MONTH(DateTime), YEAR(DateTime), Line, FGsCode,
        CASE
            WHEN DATEPART(hh, DateTime) < 6 THEN 1
            WHEN DATEPART(hh, DateTime) < 14 THEN 2
            ELSE 3
        END
    ORDER BY date, shift;
    """
    cursor.execute(query)
    data = cursor.fetchall()
    data_insert = []
    for row in data:
        new_row = {
            "date": row[0],
            "day": row[1],
            "month": row[2],
            "year": row[3],
            "shift": row[4],
            "sku":  row[5],
            "name": row[6].
            "line": row[7],
            "count": row[8],
            "countPass": row[9],
            "countNotgood": row[10],
        }
        data_insert.append(new_row)
    print(data_insert)

    collection.insert_many(data_insert)
    connection.close()
    
def querySqlServer():
    optimizationQueryCheckweigher("Table_Checkweigher") ## done
    optimizationQueryImageFail("Table_ImageFail") ## done
    #optimizationQueryOperating("Table_Operating")
    #optimizationQueryProduct("Table_Product")
    optimizationQueryResultCarton("Table_ResultCarton") ## done
    optimizationQueryResultDataman("Table_ResultDataman")
