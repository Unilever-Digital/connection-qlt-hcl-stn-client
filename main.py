
from app.views.view import *
from app.controls.query import *
import threading

if __name__ == "__main__":
    background_thread = threading.Timer(0, run_task_schedule)  # Start the background thread
    background_thread.daemon = True
    background_thread.start()
    homeViewQT()

