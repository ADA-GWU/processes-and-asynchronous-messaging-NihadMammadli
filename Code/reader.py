import psycopg2
import threading
import time
import os
from dotenv import load_dotenv

load_dotenv()

db_lock = threading.Lock()

def read_message(sender_name, db_connection, db_cursor):
    query = "SELECT SENDER_NAME, MESSAGE, SENT_TIME FROM ASYNC_MESSAGE WHERE RECEIVED_TIME IS NULL AND SENDER_NAME != %s FOR UPDATE SKIP LOCKED LIMIT 1"
    try:
        with db_lock:
            db_cursor.execute(query, (sender_name,))
            message = db_cursor.fetchone()

            if message:
                sender, msg, sent_time = message
                received_time = time.strftime('%Y-%m-%d %H:%M:%S')
                print(f"Sender {sender} sent '{msg}' at time {sent_time}.")

                update_query = "UPDATE ASYNC_MESSAGE SET RECEIVED_TIME = %s WHERE SENDER_NAME = %s AND MESSAGE = %s"
                db_cursor.execute(update_query, (received_time, sender, msg))
                db_connection.commit()

    except psycopg2.Error as e:
        if "no results to fetch" in str(e):
            print("No results to fetch. No messages meeting the criteria.")
        else:
            print("Error while fetching message:", e)

def reader_thread(db_connection, db_cursor, server_ip):
    sender_name = os.getenv('Your_name')
    while True:
        read_message(sender_name, db_connection, db_cursor)
        time.sleep(1)  

with open('Code/db_ips.txt', 'r') as file:
    db_server_ips = file.read().splitlines()

connections = []
cursors = []

for i, ip in enumerate(db_server_ips):
    conn = psycopg2.connect(
        database=os.getenv(f'DB_NAME_{i+1}'), 
        user=os.getenv(f'DB_USER_{i+1}'),
        password=os.getenv(f'DB_PASSWORD_{i+1}'),
        host=ip,
        port=os.getenv(f'DB_PORT_{i+1}')
    )
    cur = conn.cursor()
    connections.append(conn)
    cursors.append(cur)
    reader_thread_instance = threading.Thread(target=reader_thread, args=(conn, cur, ip))
    reader_thread_instance.start()

for t in threading.enumerate():
    if t != threading.current_thread():
        t.join()

for cur in cursors:
    cur.close()

for conn in connections:
    conn.close()
