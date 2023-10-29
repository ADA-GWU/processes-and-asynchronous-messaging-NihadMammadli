import threading
import time
import psycopg2
from queue import Queue

def send_message(sender_name, message, db_connection, db_cursor):
    query = "INSERT INTO ASYNC_MESSAGE (SENDER_NAME, MESSAGE, SENT_TIME) VALUES (%s, %s, %s)"
    current_time = time.strftime('%Y-%m-%d %H:%M:%S')
    data = (sender_name, message, current_time)

    db_cursor.execute(query, data)
    db_connection.commit()

def sender_thread(sender_name, db_connection, db_cursor, message_queue):
    while True:
        user_input = input("Enter a message (or 'exit' to quit): ")
        if user_input.lower() == 'exit':
            break
        
        message_queue.put(user_input)  

        message = message_queue.get()
        send_message(sender_name, message, db_connection, db_cursor)
        message_queue.task_done()

conn = psycopg2.connect(
    database="postgres",
    # user="myuser",
    # password="mypassword", 
    user="nihad",
    password="nihad",
    host="localhost",
    port="5432"
)

cur = conn.cursor()

db_server_ips = ["127.0.0.1"] 

message_queue = Queue()

sender_threads = []
for i, db_ip in enumerate(db_server_ips):
    sender_name = f"Nihad"
    sender_thread_instance = threading.Thread(target=sender_thread, args=(sender_name, conn, cur, message_queue))
    sender_threads.append(sender_thread_instance)
    sender_thread_instance.start()

for sender_thread_instance in sender_threads:
    sender_thread_instance.join()

cur.close()
conn.close()
