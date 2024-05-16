
from app.models.dbmodel import *
from app.controls.query import *
import schedule
import time

# Function to run the query periodically
def run_task_schedule():
    while True:
        try:
            querySqlServer()
            print("loop")
            time.sleep(4)  # Sleep for 4 second
        except Exception as e:
            print(e)
            time.sleep(4)
            
# Function to stop the background task
def stop_task():
    global background_thread
    if background_thread:
        background_thread.cancel()

def homeViewQT():
    """
    desktop view
    """
    from app.templates.index import HomeApp
    root = HomeApp()
    root.protocol("WM_DESTROY", stop_task)
    root.mainloop()

# pyinstaller --onefile --hidden-import schedule --hidden-import pyodbc --hidden-import openpyxl --hidden-import pymongo --hidden-import threading --hidden-import pymssql --hidden-import datetime main.py

