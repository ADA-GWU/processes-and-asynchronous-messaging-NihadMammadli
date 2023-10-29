import threading
import time
import psycopg2
import os
from dotenv import load_dotenv
from queue import Queue

load_dotenv()

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

with open('Code/db_ips.txt', 'r') as file:
    db_server_ips = file.read().splitlines()

all_connections = []
all_cursors = []
all_threads = []

for i, db_ip in enumerate(db_server_ips):
    conn = psycopg2.connect(
        database=os.getenv(f'DB_NAME_{i+1}'),
        user=os.getenv(f'DB_USER_{i+1}'),
        password=os.getenv(f'DB_PASSWORD_{i+1}'),
        host=os.getenv(f'DB_HOST_{i+1}'),
        port=os.getenv(f'DB_PORT_{i+1}')
    )
    cur = conn.cursor()
    all_connections.append(conn)
    all_cursors.append(cur)

    sender_name = os.getenv('Your_name')
    message_queue = Queue()
    sender_thread_instance = threading.Thread(target=sender_thread, args=(sender_name, conn, cur, message_queue))
    all_threads.append(sender_thread_instance)
    sender_thread_instance.start()

for thread in all_threads:
    thread.join()

for cur in all_cursors:
    cur.close()

for conn in all_connections:
    conn.close()
