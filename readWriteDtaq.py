import time
import threading
from mapepire_python.data_types import DaemonServer
from mapepire_python.client.sql_job import SQLJob
import configparser

config = configparser.ConfigParser()
config.read('mapepire.ini')
section_mapepire = "mapepire"
section_data_queues = "data_queues"
creds = DaemonServer(
    host=config[section_mapepire]["SERVER"],
    port=config[section_mapepire]["PORT"],
    user=config[section_mapepire]["USER"],
    password=config[section_mapepire]["PASSWORD"],
    ignoreUnauthorized=True
)

# Configuration for data queues

INPUT_QUEUE = config[section_data_queues]["INPUT_QUEUE"]
INPUT_QUEUE_LIB = config[section_data_queues]["INPUT_QUEUE_LIB"]
OUTPUT_QUEUE = config[section_data_queues]["OUTPUT_QUEUE"] 
OUTPUT_QUEUE_LIB = config[section_data_queues]["OUTPUT_QUEUE_LIB"]
hit_message = False

jobs = []
num_jobs = 6
for i in range(num_jobs):
    job = SQLJob()
    job.connect(creds)
    jobs.append(job)

count = 0

def process_message(message):
    # Dummy processing function - replace with actual processing logic
    return int(message) + 1

def receive_data_queue():
    global jobs
    global count
    job = jobs[count % len(jobs)]
    count += 1
    
    command_string = (
    f"SELECT * FROM TABLE(QSYS2.RECEIVE_DATA_QUEUE("
    f"DATA_QUEUE => '{INPUT_QUEUE}', "
    f"DATA_QUEUE_LIBRARY => '{INPUT_QUEUE_LIB}'))"
    )

    try:   
        result_set = job.query_and_run(command_string)
        data = result_set['data'][0]['MESSAGE_DATA_UTF8']
        if len(data) > 10:
            return (data[0:10], data[10:])
    except:
        return None

def send_data_queue(key, message):
    global jobs
    global count
    job = jobs[count % len(jobs)]
    count += 1

    command_string = (
    f"CALL QSYS2.SEND_DATA_QUEUE_UTF8("
    f"MESSAGE_DATA => '{message}', "
    f"DATA_QUEUE => '{OUTPUT_QUEUE}', "
    f"DATA_QUEUE_LIBRARY => '{OUTPUT_QUEUE_LIB}', "
    f"KEY_DATA => '{key}')"
    )
    job.query_and_run(command_string)
    print("Message sent to output queue:", message)

def write_message(key, data):
    processed_message = process_message(data)
    send_data_queue(key, processed_message)
    
def read_and_write():
    global hit_message
    message = receive_data_queue()
    if message:
        hit_message = True
        t1 = threading.Thread(None, write_message, None, message)
        t1.start()
    else:
        hit_message = False
    
def main():
    while True:
        read_and_write()
        if (hit_message == False):
            time.sleep(5)  # Adjust wait time as needed
        else:
            time.sleep(0.025) # Adjust wait time as need

if __name__ == "__main__":
    main()
