import os
import sys
path = '/home/christnp/Development/e6893/homework/e6893-project/src/usheatmap/.tmp/'
os.chdir(path)
cur_names = os.listdir(path)
for cur_name in cur_names:
    new_name = '{}.json'.format(cur_name.split(" ")[0])
    print("{} --> {}".format(cur_name,new_name))
    os.rename(cur_name,new_name)