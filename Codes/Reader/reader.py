import psycopg2
import threading
import time

def read_message(sender_name, db_connection, db_cursor):
    query = "SELECT SENDER_NAME, MESSAGE, SENT_TIME FROM ASYNC_MESSAGE WHERE RECEIVED_TIME IS NULL AND SENDER_NAME != %s FOR UPDATE SKIP LOCKED LIMIT 1"
    
    try:
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

def reader_thread(db_connection, db_cursor):
    while True:
        for i in range(len(db_server_ips)):
            sender_name = f"Sender_{i}"
            read_message(sender_name, db_connection, db_cursor)
            time.sleep(1)  # Adjust sleep time as needed

conn = psycopg2.connect(
    database="postgres",
    user="myuser",
    password="mypassword",
    host="localhost",
    port="5432"
)

cur = conn.cursor()

db_server_ips = ["127.0.0.1"] 

reader_threads = []
for _ in range(5):  
    reader_thread_instance = threading.Thread(target=reader_thread, args=(conn, cur))
    reader_threads.append(reader_thread_instance)
    reader_thread_instance.start()

for reader_thread_instance in reader_threads:
    reader_thread_instance.join()

cur.close()
conn.close()
