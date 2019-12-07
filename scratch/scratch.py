from datetime import datetime

# date = datetime(2016, 1, 1).strftime('%Y-%m-%d')
# result = datetime.strptime(date, '%G-%V-%w').date()
# result = datetime.strptime(str(date), '%Y-%W-%w').date()
# print("old date: {}".format(date))
# print("new date: {}".format(result))
for month in range(1,12):
    for day in range(1,30):
        print("U:{}, W:{}".format(datetime(2016,month,day).strftime("%U"),\
                                    datetime(2016, month,day).strftime("%W")))