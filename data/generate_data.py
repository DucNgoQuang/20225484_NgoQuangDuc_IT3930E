import random
import csv
import time
csv_file_path = 'data/Retail_Transaction_Dataset.csv'


def random_data():
    csv_file = csv.reader(open(csv_file_path))
    csv_data = list(csv_file)
    
    cols = csv_data[0]
    data = csv_data[random.randint(1, len(csv_data) - 1)]
    
    int_cols = ['CustomerID', 'Quantity']
    json_data = {}
    for i in range(len(cols)):
        if cols[i] in int_cols:
            json_data[cols[i]] = int(data[i])
        elif cols[i] == 'DiscountApplied(%)' or cols[i] == 'TotalAmount' or cols[i] == 'Price':
            json_data[cols[i]] = float(data[i])
        else:
            json_data[cols[i]] = data[i]

    json_data["TransactionDate"] = time.strftime("%m/%d/%Y %H:%M:%S", time.localtime())
    return json_data

