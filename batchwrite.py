import threading, Queue, json, datetime, sys, time, os, pymongo
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError


# method to calculate the operation duration to the nearest whole second
def duration(starttime):
    endtime = datetime.datetime.now()
    duration = endtime - starttime
    print(divmod(duration.days * 86400 + duration.seconds, 60))


try:
    # Set the source file
    filename = sys.argv[1]
    # Set the number of threads
    n_thread = int(sys.argv[2])
    # Set the database batch process size
    n_batch = int(sys.argv[3])

except:
    print("Usage: <filename> <threads> <batchsize>")
    exit()


# Create the queue
queue = Queue.Queue()

# Connect to the database
client = MongoClient(
    "mongodb://lucasamos:beadle10@ds223019.mlab.com:23019/testrecords", maxPoolSize=50)
db = client.testrecords


class ThreadClass(threading.Thread):

    def __init__(self, queue):
        threading.Thread.__init__(self)
        # Assign queue to the thread
        self.queue = queue

    def run(self):

        while True:
            objects = []

            # Insert n_batch records into the database
            for i in range(n_batch):
                objects.append(self.queue.get())
                # signal to queue the record has been processed
                self.queue.task_done()

            # Insert the queued objects into the database
            db.records.insert_many(objects)



# Open access to the json file
try:
    content = json.load(open(filename, "r"))
except:
    print("An error occurred when opening file '" + filename + "'")
    exit()

# Iterate over the file and store each record in the queue
for jsonobj in content:
    queue.put(jsonobj)


# Start the timer
starttime = datetime.datetime.now()


# Create and start the threads
threads = []
for i in range(n_thread):
    t = ThreadClass(queue)
    t.setDaemon(True)
    # Start thread
    t.start()


# Block until everything in the queue has been processed
queue.join()

# print the duration of the operation in seconds
duration(starttime)
