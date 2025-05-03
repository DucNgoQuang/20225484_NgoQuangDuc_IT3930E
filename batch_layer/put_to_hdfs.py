from hdfs import InsecureClient
import datetime
import pandas as pd

HDFS_HOST = 'localhost'
HDFS_PORT = 9870
USER = 'hdoop'

def load_to_hdfs(data, columns, csv_file_name, date_col = None):
    if date_col:
        data = eval(data)
        trasaction_date = data[date_col]
        date = datetime.datetime.strptime(trasaction_date, '%m/%d/%Y %H:%M').strftime('%d%m%y')
    else:
        date = datetime.datetime.now().strftime('%d%m%y')

    file_path = f'{csv_file_name}_{date}.csv'
    client = InsecureClient(f'http://{HDFS_HOST}:{HDFS_PORT}', user=USER)
    try : 
        new_df = pd.DataFrame([data], columns=columns)
        if client.content(file_path, strict=False):
            with client.read(file_path) as reader:
                df = pd.read_csv(reader)
                df = pd.concat([df, new_df], ignore_index=True)
            client.write(file_path, df.to_csv(index=False), overwrite=True, permission='777')
        else:   
            with client.write(file_path, overwrite=True, permission='777') as writer:
                new_df.to_csv(writer, index=False)
        print("Data written to HDFS successfully.")
    except Exception as e:
        print(f"Error while writing to HDFS: {e}")
        return False