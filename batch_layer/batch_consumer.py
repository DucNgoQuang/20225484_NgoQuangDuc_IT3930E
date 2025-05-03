from kafka import KafkaConsumer
from put_to_hdfs import load_to_hdfs
import threading
from hdfs import InsecureClient
import datetime
import pandas as pd

HDFS_HOST = 'localhost'
HDFS_PORT = 9870
USER = 'hdoop'



def consumer_thread():
    columns = [
        'CustomerID', 'ProductID', 'Quantity', 'Price',
        'TransactionDate', 'PaymentMethod', 'StoreLocation',
        'ProductCategory', 'DiscountApplied(%)', 'TotalAmount'
    ]
    csv_file_name = 'retail_transactions'
    date_col = 'TransactionDate'

    consumer = KafkaConsumer(
        'retail_transactions',
        bootstrap_servers='localhost:9092'
    )

    today = datetime.datetime.now().strftime('%d%m%y')
    
    today_file_path = f'{csv_file_name}_{today}'
    client = InsecureClient(f'http://{HDFS_HOST}:{HDFS_PORT}', user=USER)

    # if not client.content(today_file_path, strict=False):
    #     client.write(today_file_path, data='', overwrite=True, permission='777')
    df = pd.DataFrame([], columns=columns)
    # else :
    #     with client.read(today_file_path) as reader:
    #         df = pd.read_csv(reader, names=columns)
    try : 
        for message in consumer:
            data = eval(message.value.decode('utf-8'))
        
            transaction_date = data[date_col]
            date = datetime.datetime.strptime(transaction_date, '%m/%d/%Y %H:%M').strftime('%d%m%y')
        
            new_df = pd.DataFrame([data], columns=columns)
        
            if date == today:
                df = pd.concat([df, new_df], ignore_index=True)
            else:
                client.write(f'{csv_file_name}_{date}/part-0.csv', new_df.to_csv(index=False), overwrite=False, permission='777')               
                break
            if df.shape[0] > 1000:
                timestamp = datetime.datetime.now().strftime('%H%M%S')
                file_path = today_file_path + f"/part-{timestamp}.csv"
        
                if client.content(file_path, strict=False):
                    with client.write(today_file_path, append= True) as writer:
                        new_df.to_csv(writer, index=False, header=False, mode='a')
                        writer.flush()
                else:
                    client.write(file_path, df.to_csv(index=False,header=False), overwrite=False, permission='777') # write every 1000 rows
        
                df = pd.DataFrame([], columns=columns)
            
    except Exception as e:
        print(f"Error while writing to HDFS: {e}")
    finally:

        timestamp = datetime.datetime.now().strftime('%H%M%S')
        file_path = today_file_path + f"/part-{timestamp}.csv"
        
        client.write(file_path, df.to_csv(index=False,header= False), overwrite= False, permission='777')

c_thread = threading.Thread(target=consumer_thread) 

c_thread.start()

c_thread.join()