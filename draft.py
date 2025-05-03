import datetime
date = datetime.datetime.now().strftime('%d/%m/%y')
date = date.replace('/', '')
print(date)