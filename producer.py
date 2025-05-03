from kafka import KafkaProducer
from data.generate_data import random_data 
import threading
import time

def producer_thread(): 
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    while True : 
        try : 
            time.sleep(5)
            data = random_data()
            print(data)
            producer.send('retail_transactions', str(data).encode('utf-8'))
            print('Data sent !')
            producer.flush()
        except Exception as e:
            print(f"Error while sending {data}: {e}")
            continue

p_thread = threading.Thread(target=producer_thread)

p_thread.start()

p_thread.join()