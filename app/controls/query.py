import datetime
import time
from datetime import datetime as dt
from app.controls.control import *


""" 
Table_Data
Table_ImageFail_Barcode
Table_ImageFail_Cap1
Table_ImageFail_Cap2
Table_ImageFail_DateCode
Table_ImageFail_LO1
Table_ImageFail_LO2
Table_Product
"""

def optimizationQueryData(table):
    """
    table data 
    """
    connection = connectToSqlServer('DESKTOP-DGEHS9H', 'U-CheckDate-Barcode')
    cursor = connection.cursor()
    collection = ensure_collection_exists("U-CheckDate-Barcode-Stn",table)
    
    result = collection.delete_many({})
    startdate = datetime.datetime(2023, 1, 1, 0, 0, 0)
    query = f"""
    SELECT CONVERT(date, Datetime) AS date,
        DAY(Datetime) AS day,
        MONTH(Datetime) AS month,
        YEAR(Datetime) AS year,
		CASE
            WHEN DATEPART(hh, DateTime) < 6 THEN 1
            WHEN DATEPART(hh, DateTime) < 14 THEN 2
            ELSE 3
        END AS shift,
		FgsCode AS sku,
        COUNT(*) AS count,
        SUM(CASE WHEN Barcode = 'Good' THEN 1 ELSE 0 END) AS BarcodeGood,
        SUM(CASE WHEN DateCode = 'Good' THEN 1 ELSE 0 END) AS DatecodeGood,
        SUM(CASE WHEN Cap1 = 'Good' THEN 1 ELSE 0 END) AS Cap1Good,
        SUM(CASE WHEN Cap2 = 'Good' THEN 1 ELSE 0 END) AS Cap2Good,
        SUM(CASE WHEN Liquid1 = 'Good' THEN 1 ELSE 0 END) AS Liquid1Good,
        SUM(CASE WHEN Liquid2 = 'Good' THEN 1 ELSE 0 END) AS Liquid2Good
    FROM {table}
    GROUP BY CONVERT(date, Datetime), DAY(Datetime), MONTH(Datetime), YEAR(Datetime), FgsCode,
            CASE
                WHEN DATEPART(hh, Datetime) < 6 THEN 1
                WHEN DATEPART(hh, Datetime) < 14 THEN 2
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
            "barcodeGood": row[6],
            "datecodeGood": row[7],
            "cap1good": row[8],
            "cap2good": row[9],
            "liquid1good": row[10],
            "liquid2good": row[11],
        }
        data_insert.append(new_row)

    collection.insert_many(data_insert)
    connection.close()

def optimizationQueryImageFailBarcode(table):
    """
    counter bottles server
    """
    connection = connectToSqlServer('DESKTOP-DGEHS9H', 'U-CheckDate-Barcode')
    cursor = connection.cursor()
    collection = ensure_collection_exists("U-CheckDate-Barcode-Stn", table)
    collection.delete_many({})
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
		Line AS line,
        COUNT(*) AS countFail
    FROM {table}
    GROUP BY CONVERT(date, DateTime), DAY(DateTime), MONTH(DateTime), YEAR(DateTime), FGsCode, Line,
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
            "countFail": row[7],
        }
        data_insert.append(new_row)

    collection.insert_many(data_insert)
    connection.close()

def optimizationQueryImageFailCap1(table):
    """
    counter bottles server
    """
    connection = connectToSqlServer('DESKTOP-DGEHS9H', 'U-CheckDate-Barcode')
    cursor = connection.cursor()
    collection = ensure_collection_exists("U-CheckDate-Barcode-Stn", table)

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
		Line AS line,
        COUNT(*) AS countFail
    FROM {table}
    GROUP BY CONVERT(date, DateTime), DAY(DateTime), MONTH(DateTime), YEAR(DateTime), FGsCode, Line,
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
            "countFail": row[7],
        }
        # for key, value in new_row.items():
        # if key == "date":
        # new_row[key] = dt.strftime(value, "%Y-%m-%d")
        # else:
        # new_row[key] = value if value != float('nan') else ""
        data_insert.append(new_row)

    collection.insert_many(data_insert)
    connection.close()

def optimizationQueryImageFailCap2(table):
    """
    counter bottles server
    """
    connection = connectToSqlServer('DESKTOP-DGEHS9H', 'U-CheckDate-Barcode')
    cursor = connection.cursor()
    collection = ensure_collection_exists("U-CheckDate-Barcode-Stn", table)
    
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
		Line AS line,
        COUNT(*) AS countFail
    FROM {table}
    GROUP BY CONVERT(date, DateTime), DAY(DateTime), MONTH(DateTime), YEAR(DateTime), FGsCode, Line,
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
            "countFail": row[7],
        }
        #for key, value in new_row.items():
            #if key == "date":
                #new_row[key] = dt.strftime(value, "%Y-%m-%d")
            #else:
                #new_row[key] = value if value != float('nan') else ""
        data_insert.append(new_row)

    collection.insert_many(data_insert)
    connection.close()

def optimizationQueryImageFailDateCode(table):
    """
    result carton
    """
    connection = connectToSqlServer('DESKTOP-DGEHS9H', 'U-CheckDate-Barcode')
    cursor = connection.cursor()
    collection = ensure_collection_exists("U-CheckDate-Barcode-Stn", table)
    
    result = collection.delete_many({})
    startdate = datetime.datetime(2023, 1, 1, 0, 0, 0)
    query = f"""
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
		Line AS line,
        COUNT(*) AS countFail
    FROM {table}
    GROUP BY CONVERT(date, DateTime), DAY(DateTime), MONTH(DateTime), YEAR(DateTime), FGsCode, Line,
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
            "countFail": row[7],
        }
        data_insert.append(new_row)

    collection.insert_many(data_insert)
    connection.close()

def optimizationQueryImageFailLO1(table):
    """
    counter bottles server
    """
    connection = connectToSqlServer('DESKTOP-DGEHS9H', 'U-CheckDate-Barcode')
    cursor = connection.cursor()
    collection = ensure_collection_exists("U-CheckDate-Barcode-Stn", table)
    
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
		Line AS line,
        COUNT(*) AS countFail
    FROM {table}
    GROUP BY CONVERT(date, DateTime), DAY(DateTime), MONTH(DateTime), YEAR(DateTime), FGsCode, Line,
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
            "countFail": row[7],
        }
        #for key, value in new_row.items():
            #if key == "date":
                #new_row[key] = dt.strftime(value, "%Y-%m-%d")
            #else:
                #new_row[key] = value if value != float('nan') else ""
        data_insert.append(new_row)

    collection.insert_many(data_insert)
    connection.close()
    
def optimizationQueryImageFailLO2(table):
    """
    counter bottles server
    """
    connection = connectToSqlServer('DESKTOP-DGEHS9H', 'U-CheckDate-Barcode')
    cursor = connection.cursor()
    collection = ensure_collection_exists("U-CheckDate-Barcode-Stn", table)
    
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
		Line AS line,
        COUNT(*) AS countFail
    FROM {table}
    GROUP BY CONVERT(date, DateTime), DAY(DateTime), MONTH(DateTime), YEAR(DateTime), FGsCode, Line,
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
            "countFail": row[7],
        }
        #for key, value in new_row.items():
            #if key == "date":
                #new_row[key] = dt.strftime(value, "%Y-%m-%d")
            #else:
                #new_row[key] = value if value != float('nan') else ""
        data_insert.append(new_row)

    collection.insert_many(data_insert)
    connection.close()

def querySqlServer():
    optimizationQueryData("Table_Data") ## done
    optimizationQueryImageFailBarcode("Table_ImageFail_Barcode") ## done
    optimizationQueryImageFailCap1("Table_ImageFail_Cap1")## done
    optimizationQueryImageFailCap2("Table_ImageFail_Cap2")## done
    optimizationQueryImageFailDateCode("Table_ImageFail_DateCode")
    optimizationQueryImageFailLO1("Table_ImageFail_LO1") 
    optimizationQueryImageFailLO2("Table_ImageFail_LO2")
