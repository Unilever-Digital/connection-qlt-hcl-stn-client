import pyodbc
import json
import pymongo


def connectToSqlServer(server, database):
    """
    Connect to local SQL Server database using Windows Authentication
    """
    try:
        conn = pyodbc.connect(driver="{SQL Server}",
                              server=server,
                              database=database,
                              Trusted_Connection="yes")
        return conn
    except Exception as e:
        print("Error connecting to SQL Server:", e)
        return None
    
def cursorDatabase(database):
    """
    Connects to MongoDB and returns the database. If the database does not exist, it will be created.
    """
    try:
        uri = "mongodb+srv://unilever-digital:U2024-digital@cluster0.ixcliyp.mongodb.net/"
        conn = pymongo.MongoClient(uri)

        if database in conn.list_database_names():
            print(f"Database '{database}' already exists.")
        else:
            db = conn[database]
            db.create_collection("dummy_collection")
            db.dummy_collection.insert_one({"dummy_key": "dummy_value"})
            db.dummy_collection.drop()
            
        return conn[database]
    except Exception as e:
        print("Error connecting to MongoDB:", e)
        return None
    
def ensure_collection_exists(database_name, collection_name):
    """
    Check if a collection exists in the database. If not, create it.
    """
    try:
        database = cursorDatabase(database_name)
        if collection_name in database.list_collection_names():
            print(f"Collection '{collection_name}' already exists.")
        else:
            print(f"Collection '{collection_name}' does not exist. Creating collection '{collection_name}'.")
            database.create_collection(collection_name)
        return database[collection_name]
    except Exception as e:
        print(f"Error handling collection '{collection_name}':", e)
        return None

def noSqlTransform(rows):
    """tranform Sql table to tree Node json

    Args:
        table (dataframe)): sql table
    """
    try:
    
        # Convert rows to a list of dictionaries
        results = []
        for row in rows:
            results.append(dict(row))

        # Convert the list of dictionaries to JSON
        return json.dumps(results)
    except Exception as e:
        print(e)